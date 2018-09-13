use std::collections::{HashMap,HashSet};
use std::io::{self,Read,Write,ErrorKind};
use std::rc::{Rc,Weak};
use std::cell::RefCell;
use std::thread::{self,Thread,Builder};
use std::os::unix::io::RawFd;
use std::os::unix::io::{FromRawFd,AsRawFd,IntoRawFd};
use std::sync::mpsc::{self,channel,Receiver};
use std::net::{SocketAddr,IpAddr,Shutdown};
use std::str::{FromStr, from_utf8, from_utf8_unchecked};
use mio::*;
use mio::net::*;
use mio_uds::UnixStream;
use mio::unix::UnixReady;
use uuid::Uuid;
use nom::HexDisplay;
use rand::random;
use time::{SteadyTime,Duration};
use slab::{Entry,VacantEntry,Slab};
use mio_extras::timer::{Timer, Timeout};
use backtrace::Backtrace;

use sozu_command::channel::Channel;
use sozu_command::state::ConfigState;
use sozu_command::scm_socket::{Listeners,ScmSocket};
use sozu_command::messages::{self,Application,Order,HttpFront,HttpListener,OrderMessage,OrderMessageAnswer,OrderMessageStatus, LoadBalancingParams};
use sozu_command::logging;

use network::{AppId,Backend,ClientResult,ConnectionError,RequiredEvents,Protocol,Readiness,SessionMetrics,
  ProxyClient,ProxyConfiguration,AcceptError,BackendConnectAction,BackendConnectionStatus,
  CloseResult};
use network::backends::BackendMap;
use network::pool::{Pool,Checkout,Reset};
use network::buffer_queue::BufferQueue;
use network::protocol::{ProtocolResult,StickySession,TlsHandshake,Http,Pipe};
use network::protocol::http::DefaultAnswerStatus;
use network::protocol::proxy_protocol::expect::ExpectProxyProtocol;
use network::proxy::{Server,ProxyChannel,ListenToken,ListenPortState,ClientToken,ListenClient, CONN_RETRIES};
use network::socket::{SocketHandler,SocketResult,server_bind};
use network::retry::RetryPolicy;
use parser::http11::{hostname_and_port, RequestState};
use network::trie::TrieNode;
use network::tcp;
use util::UnwrapLog;
use network::https_rustls;

type BackendToken = Token;

#[derive(PartialEq)]
pub enum ClientStatus {
  Normal,
  DefaultAnswer,
}

pub enum State {
  Expect(ExpectProxyProtocol<TcpStream>),
  Http(Http<TcpStream>),
  WebSocket(Pipe<TcpStream>)
}

pub struct Client {
  frontend_token:     Token,
  backend:            Option<Rc<RefCell<Backend>>>,
  back_connected:     BackendConnectionStatus,
  protocol:           Option<State>,
  pool:               Weak<RefCell<Pool<BufferQueue>>>,
  sticky_session:     bool,
  metrics:            SessionMetrics,
  pub app_id:         Option<String>,
  sticky_name:        String,
  front_timeout:      Timeout,
  last_event:         SteadyTime,
  pub listen_token:   Token,
  connection_attempt: u8,
  closed: bool,
}

impl Client {
  pub fn new(sock: TcpStream, token: Token, pool: Weak<RefCell<Pool<BufferQueue>>>,
    public_address: Option<IpAddr>, expect_proxy: bool, sticky_name: String, timeout: Timeout,
    listen_token: Token) -> Option<Client> {
    let protocol = if expect_proxy {
      trace!("starting in expect proxy state");
      gauge_add!("protocol.proxy.expect", 1);
      Some(State::Expect(ExpectProxyProtocol::new(sock, token)))
    } else {
      gauge_add!("protocol.http", 1);
      Http::new(sock, token, pool.clone(), public_address,None, sticky_name.clone(), Protocol::HTTP).map(|http| State::Http(http))
    };

    protocol.map(|pr| {
      let request_id = Uuid::new_v4().hyphenated().to_string();
      let log_ctx    = format!("{}\tunknown\t", &request_id);
      let mut client = Client {
        backend:            None,
        back_connected:     BackendConnectionStatus::NotConnected,
        protocol:           Some(pr),
        frontend_token:     token,
        pool:               pool,
        sticky_session:     false,
        metrics:            SessionMetrics::new(),
        app_id:             None,
        sticky_name:        sticky_name,
        front_timeout:      timeout,
        last_event:         SteadyTime::now(),
        listen_token,
        connection_attempt: 0,
        closed: false,
      };

      client.front_readiness().interest = UnixReady::from(Ready::readable()) | UnixReady::hup() | UnixReady::error();

      client
    })
  }

  pub fn upgrade(&mut self) -> bool {
    debug!("HTTP::upgrade");
    let protocol = unwrap_msg!(self.protocol.take());
    if let State::Http(http) = protocol {
      debug!("switching to pipe");
      let front_token = self.frontend_token;
      let back_token  = unwrap_msg!(http.back_token());

      let front_buf = match http.front_buf {
        Some(buf) => buf,
        None => if let Some(p) = self.pool.upgrade() {
          if let Some(buf) = p.borrow_mut().checkout() {
            buf
          } else {
            return false;
          }
        } else {
          return false;
        }
      };

      let back_buf = match http.back_buf {
        Some(buf) => buf,
        None => if let Some(p) = self.pool.upgrade() {
          if let Some(buf) = p.borrow_mut().checkout() {
            buf
          } else {
            return false;
          }
        } else {
          return false;
        }
      };

      gauge_add!("protocol.http", -1);
      gauge_add!("protocol.ws", 1);
      gauge_add!("http.active_requests", -1);
      let mut pipe = Pipe::new(http.frontend, front_token, Some(unwrap_msg!(http.backend)),
        front_buf, back_buf, http.public_address);

      pipe.front_readiness.event = http.front_readiness.event;
      pipe.back_readiness.event  = http.back_readiness.event;
      pipe.set_back_token(back_token);

      self.protocol = Some(State::WebSocket(pipe));
      true
    } else if let State::Expect(expect) = protocol {
      debug!("switching to HTTP");
      let readiness = expect.readiness;
      if let Some((Some(public_address), Some(client_address))) = expect.addresses.as_ref().map(|add| {
        (add.destination().clone(), add.source().clone())
      }) {
        let http = Http::new(expect.frontend, expect.frontend_token,
          self.pool.clone(), Some(public_address.ip()), Some(client_address),
          self.sticky_name.clone(), Protocol::HTTP).map(|mut http| {
            http.front_readiness.event = readiness.event;

            State::Http(http)
        });

        if http.is_none() {
          //we cannot put back the protocol since we moved the stream
          //self.protocol = Some(State::Expect(expect));
          return false;
        }

        gauge_add!("protocol.proxy.expect", -1);
        gauge_add!("protocol.http", 1);
        self.protocol = http;
        return true;
      }

      //we cannot put back the protocol since we moved the stream
      //self.protocol = Some(State::Expect(expect));
      false
    } else {
      self.protocol = Some(protocol);
      true
    }
  }

  pub fn set_answer(&mut self, answer: DefaultAnswerStatus, buf: Rc<Vec<u8>>)  {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http) => http.set_answer(answer, buf),
      _ => {}
    }
  }

  pub fn http(&mut self) -> Option<&mut Http<TcpStream>> {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http) => Some(http),
      _ => None
    }
  }

  fn front_hup(&mut self) -> ClientResult {
    ClientResult::CloseClient
  }

  fn back_hup(&mut self) -> ClientResult {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http)      => http.back_hup(),
      State::WebSocket(ref mut pipe) => pipe.back_hup(),
      _                              => ClientResult::CloseClient,
    }
  }

  fn log_context(&self) -> String {
    match *unwrap_msg!(self.protocol.as_ref()) {
      State::Http(ref http) => {
        if let Some(ref app_id) = http.app_id {
          format!("{}\t{}\t", http.request_id, app_id)
        } else {
          format!("{}\tunknown\t", http.request_id)
        }

      },
      _ => "".to_string()
    }
  }

  // Read content from the client
  fn readable(&mut self) -> ClientResult {
    let (upgrade, result) = match *unwrap_msg!(self.protocol.as_mut()) {
      State::Expect(ref mut expect)  => expect.readable(&mut self.metrics),
      State::Http(ref mut http)      => (ProtocolResult::Continue, http.readable(&mut self.metrics)),
      State::WebSocket(ref mut pipe) => (ProtocolResult::Continue, pipe.readable(&mut self.metrics)),
    };

    if upgrade == ProtocolResult::Continue {
      result
    } else {
      if self.upgrade() {
        match *unwrap_msg!(self.protocol.as_mut()) {
          State::Http(ref mut http) => http.readable(&mut self.metrics),
          _ => result
        }
      } else {
        error!("failed protocol upgrade");
        ClientResult::CloseClient
      }
    }
  }

  // Forward content to client
  fn writable(&mut self) -> ClientResult {
    match  *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http)      => http.writable(&mut self.metrics),
      State::WebSocket(ref mut pipe) => pipe.writable(&mut self.metrics),
      State::Expect(_)               => ClientResult::CloseClient,
    }
  }

  // Forward content to application
  fn back_writable(&mut self) -> ClientResult {
    match *unwrap_msg!(self.protocol.as_mut())  {
      State::Http(ref mut http)      => http.back_writable(&mut self.metrics),
      State::WebSocket(ref mut pipe) => pipe.back_writable(&mut self.metrics),
      State::Expect(_)               => ClientResult::CloseClient,
    }
  }

  // Read content from application
  fn back_readable(&mut self) -> ClientResult {
    let (upgrade, result) = match  *unwrap_msg!(self.protocol.as_mut())  {
      State::Http(ref mut http)      => http.back_readable(&mut self.metrics),
      State::WebSocket(ref mut pipe) => (ProtocolResult::Continue, pipe.back_readable(&mut self.metrics)),
      State::Expect(_)               => return ClientResult::CloseClient,
    };

    if upgrade == ProtocolResult::Continue {
      result
    } else {
      if self.upgrade() {
        match *unwrap_msg!(self.protocol.as_mut()) {
          State::WebSocket(ref mut pipe) => pipe.back_readable(&mut self.metrics),
          _ => result
        }
      } else {
        error!("failed protocol upgrade");
        ClientResult::CloseClient
      }
    }
  }

  fn front_socket(&self) -> &TcpStream {
    match *unwrap_msg!(self.protocol.as_ref()) {
      State::Http(ref http)      => http.front_socket(),
      State::WebSocket(ref pipe) => pipe.front_socket(),
      State::Expect(ref expect)  => expect.front_socket(),
    }
  }

  fn back_socket(&self)  -> Option<&TcpStream> {
    match *unwrap_msg!(self.protocol.as_ref()) {
      State::Http(ref http)      => http.back_socket(),
      State::WebSocket(ref pipe) => pipe.back_socket(),
      State::Expect(_)           => None,
    }
  }

  fn back_token(&self)   -> Option<Token> {
    match *unwrap_msg!(self.protocol.as_ref()) {
      State::Http(ref http)      => http.back_token(),
      State::WebSocket(ref pipe) => pipe.back_token(),
      State::Expect(_)           => None,
    }
  }

  fn set_back_socket(&mut self, socket: TcpStream) {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http)      => http.set_back_socket(socket),
      State::WebSocket(ref mut pipe) => {} /*pipe.set_back_socket(unwrap_msg!(socket.try_clone()))*/
      State::Expect(_)               => {},
    }
  }

  fn set_back_token(&mut self, token: Token) {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http)      => http.set_back_token(token),
      State::WebSocket(ref mut pipe) => pipe.set_back_token(token),
      State::Expect(_)               => {},
    }
  }

  fn back_connected(&self)     -> BackendConnectionStatus {
    self.back_connected
  }

  fn set_back_connected(&mut self, connected: BackendConnectionStatus) {
    self.back_connected = connected;
    if connected == BackendConnectionStatus::Connected {
      gauge_add!("backend.connections", 1);
      self.backend.as_ref().map(|backend| {
        let ref mut backend = *backend.borrow_mut();
        //successful connection, rest failure counter
        backend.failures = 0;
        backend.retry_policy.succeed();
      });
    }
  }

  fn metrics(&mut self)        -> &mut SessionMetrics {
    &mut self.metrics
  }

  fn remove_backend(&mut self) -> (Option<String>, Option<SocketAddr>) {
    debug!("{}\tPROXY [{} -> {}] CLOSED BACKEND",
      self.http().map(|h| h.log_ctx.clone()).unwrap_or("".to_string()), self.frontend_token.0,
      self.back_token().map(|t| format!("{}", t.0)).unwrap_or("-".to_string()));

    let backend = self.backend.take();
    if backend.is_some() {
      let addr:Option<SocketAddr> = self.back_socket().and_then(|sock| sock.peer_addr().ok());
      self.http().map(|h| h.clear_back_token());

      (self.app_id.clone(), addr)
    } else {
      (None, None)
    }
  }

  fn front_readiness(&mut self) -> &mut Readiness {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http)      => &mut http.front_readiness,
      State::WebSocket(ref mut pipe) => &mut pipe.front_readiness,
      State::Expect(ref mut expect)  => &mut expect.readiness,
    }
  }

  fn back_readiness(&mut self) -> Option<&mut Readiness> {
    match *unwrap_msg!(self.protocol.as_mut()) {
      State::Http(ref mut http)      => Some(&mut http.back_readiness),
      State::WebSocket(ref mut pipe) => Some(&mut pipe.back_readiness),
      _ => None,
    }
  }

  fn reset_connection_attempt(&mut self) {
    self.connection_attempt = 0;
  }
}

impl ProxyClient for Client {
  fn close(&mut self, poll: &mut Poll) -> CloseResult {
    self.metrics.service_stop();
    self.front_socket().shutdown(Shutdown::Both);
    poll.deregister(self.front_socket());

    let mut result = CloseResult::default();

    if let Some(tk) = self.back_token() {
      result.tokens.push(tk)
    }

    if let (Some(app_id), Some(addr)) = self.remove_backend() {
      result.backends.push((app_id, addr.clone()));
    }

    let back_connected = self.back_connected();
    if back_connected != BackendConnectionStatus::NotConnected {
      if let Some(sock) = self.back_socket() {
        sock.shutdown(Shutdown::Both);
        poll.deregister(sock);
      }
    }

    if back_connected == BackendConnectionStatus::Connected {
      gauge_add!("backend.connections", -1);
    }

    self.set_back_connected(BackendConnectionStatus::NotConnected);

    if let Some(State::Http(ref http)) = self.protocol {
      //if the state was initial, the connection was already reset
      if unwrap_msg!(http.state.as_ref()).request != Some(RequestState::Initial) {
        gauge_add!("http.active_requests", -1);
      }
    }

    match self.protocol {
      Some(State::Expect(_)) => gauge_add!("protocol.proxy.expect", -1),
      Some(State::Http(_)) => gauge_add!("protocol.http", -1),
      Some(State::WebSocket(_)) => gauge_add!("protocol.ws", -1),
      None => {},
    }

    self.closed = true;
    result.tokens.push(self.frontend_token);

    result
  }

  fn timeout(&self, token: Token, timer: &mut Timer<Token>, front_timeout: &Duration) -> ClientResult {
    if self.frontend_token == token {
      let dur = SteadyTime::now() - self.last_event;
      if dur < *front_timeout {
        timer.set_timeout((*front_timeout - dur).to_std().unwrap(), token);
        ClientResult::Continue
      } else {
        ClientResult::CloseClient
      }
    } else {
      ClientResult::Continue
    }
  }

  fn cancel_timeouts(&self, timer: &mut Timer<Token>) {
    timer.cancel_timeout(&self.front_timeout);
  }

  fn close_backend(&mut self, _: Token, poll: &mut Poll) -> Option<(String,SocketAddr)> {
    let mut res = None;
    if let (Some(app_id), Some(addr)) = self.remove_backend() {
      res = Some((app_id, addr.clone()));
    }

    let back_connected = self.back_connected();
    if back_connected != BackendConnectionStatus::NotConnected {
      if let Some(sock) = self.back_socket() {
        sock.shutdown(Shutdown::Both);
        poll.deregister(sock);
      }
    }

    if back_connected == BackendConnectionStatus::Connected {
      gauge_add!("backend.connections", -1);
    }

    self.set_back_connected(BackendConnectionStatus::NotConnected);

    self.http().map(|h| h.clear_back_token());
    self.http().map(|h| h.remove_backend());
    res
  }

  fn protocol(&self) -> Protocol {
    Protocol::HTTP
  }

  fn process_events(&mut self, token: Token, events: Ready) {
    trace!("token {:?} got event {}", token, super::unix_ready_to_string(UnixReady::from(events)));
    self.last_event = SteadyTime::now();

    if self.frontend_token == token {
      self.front_readiness().event = self.front_readiness().event | UnixReady::from(events);
    } else if self.back_token() == Some(token) {
      self.back_readiness().map(|r| r.event = r.event | UnixReady::from(events));
    }
  }

  fn ready(&mut self) -> ClientResult {
    let mut counter = 0;
    let max_loop_iterations = 100000;

    self.metrics().service_start();

    if self.back_connected() == BackendConnectionStatus::Connecting {
      if self.back_readiness().map(|r| r.event.is_hup()).unwrap_or(false) {
        //retry connecting the backend
        error!("{} error connecting to backend, trying again", self.log_context());
        self.metrics().service_stop();
        self.connection_attempt += 1;

        let backend_token = self.back_token();
        self.http().map(|h| h.clear_back_token());
        self.http().map(|h| h.remove_backend());
        return ClientResult::ReconnectBackend(Some(self.frontend_token), backend_token);
      } else if self.back_readiness().map(|r| r.event != UnixReady::from(Ready::empty())).unwrap_or(false) {
        self.reset_connection_attempt();
        self.set_back_connected(BackendConnectionStatus::Connected);
      }
    }

    if self.front_readiness().event.is_hup() {
      let order = self.front_hup();
      match order {
        ClientResult::CloseClient => {
          return order;
        },
        _ => {
          self.front_readiness().event.remove(UnixReady::hup());
          return order;
        }
      }
    }

    let token = self.frontend_token.clone();
    while counter < max_loop_iterations {
      let front_interest = self.front_readiness().interest & self.front_readiness().event;
      let back_interest  = self.back_readiness().map(|r| r.interest & r.event).unwrap_or(UnixReady::from(Ready::empty()));

      trace!("PROXY\t{} {:?} {:?} -> {:?}", self.log_context(), token, self.front_readiness().clone(), self.back_readiness());

      if front_interest == UnixReady::from(Ready::empty()) && back_interest == UnixReady::from(Ready::empty()) {
        break;
      }

      if self.back_readiness().map(|r| r.event.is_hup()).unwrap_or(false) && self.front_readiness().interest.is_writable() &&
        ! self.front_readiness().event.is_writable() {
        break;
      }

      if front_interest.is_readable() {
        let order = self.readable();
        trace!("front readable\tinterpreting client order {:?}", order);

        if order != ClientResult::Continue {
          return order;
        }
      }

      if back_interest.is_writable() {
        let order = self.back_writable();
        if order != ClientResult::Continue {
          return order;
        }
      }

      if back_interest.is_readable() {
        let order = self.back_readable();
        if order != ClientResult::Continue {
          return order;
        }
      }

      if front_interest.is_writable() {
        let order = self.writable();
        trace!("front writable\tinterpreting client order {:?}", order);

        if order != ClientResult::Continue {
          return order;
        }
      }

      if back_interest.is_hup() {
        let order = self.back_hup();
        match order {
          ClientResult::CloseClient => {
            return order;
          },
          ClientResult::Continue => {},
          _ => {
            self.back_readiness().map(|r| r.event.remove(UnixReady::hup()));
            return order;
          }
        };
      }

      if front_interest.is_error() || back_interest.is_error() {
        if front_interest.is_error() {
          error!("PROXY client {:?} front error, disconnecting", self.frontend_token);
        } else {
          error!("PROXY client {:?} back error, disconnecting", self.frontend_token);
        }

        self.front_readiness().interest = UnixReady::from(Ready::empty());
        self.back_readiness().map(|r| r.interest  = UnixReady::from(Ready::empty()));
        return ClientResult::CloseClient;
      }

      counter += 1;
    }

    if counter == max_loop_iterations {
      error!("PROXY\thandling client {:?} went through {} iterations, there's a probable infinite loop bug, closing the connection", self.frontend_token, max_loop_iterations);

      let front_interest = self.front_readiness().interest & self.front_readiness().event;
      let back_interest  = self.back_readiness().map(|r| r.interest & r.event);

      let token = self.frontend_token.clone();
      let back = self.back_readiness().cloned();
      error!("PROXY\t{:?} readiness: {:?} -> {:?} | front: {:?} | back: {:?} ", token,
        self.front_readiness(), back, front_interest, back_interest);

      return ClientResult::CloseClient;
    }

    ClientResult::Continue
  }

  fn last_event(&self) -> SteadyTime {
    self.last_event
  }

  fn print_state(&self) {
    let p:String = match &self.protocol {
      Some(State::Expect(_))    => String::from("Expect"),
      Some(State::Http(h))      => format!("HTTPS: {:?}", h.state),
      Some(State::WebSocket(_)) => String::from("WTTP"),
      None                      => String::from("None"),
    };

    let rf = match *unwrap_msg!(self.protocol.as_ref()) {
      State::Expect(ref expect)  => &expect.readiness,
      State::Http(ref http)      => &http.front_readiness,
      State::WebSocket(ref pipe) => &pipe.front_readiness,
    };
    let rb = match *unwrap_msg!(self.protocol.as_ref()) {
      State::Http(ref http)      => Some(&http.back_readiness),
      State::WebSocket(ref pipe) => Some(&pipe.back_readiness),
      _ => None,
    };

    error!("zombie client[{:?} => {:?}], state => readiness: {:?} -> {:?},
      protocol: {}, app_id: {:?}, last_event: {:?},
      back_connected: {:?}, metrics: {:?}",
      self.frontend_token, self.back_token(), rf, rb, p, self.app_id, self.last_event, self.back_connected, self.metrics);
  }

  fn tokens(&self) -> Vec<Token> {
    let mut v = vec![self.frontend_token];
    if let Some(tk) = self.back_token() {
      v.push(tk)
    }

    v
  }
}

impl Drop for Client {
  fn drop(&mut self) {
    if !self.closed {
      error!("HTTP Client {:?} close() method was not called before being dropped", self.frontend_token);
      self.print_state();
      let bt = Backtrace::new();
      error!("backtrace:\n{:?}", bt);
    }
  }
}

#[allow(non_snake_case)]
pub struct DefaultAnswers {
  pub NotFound:           Rc<Vec<u8>>,
  pub ServiceUnavailable: Rc<Vec<u8>>,
  pub BadRequest:         Rc<Vec<u8>>,
}

pub type Hostname = String;

pub struct Listener {
  listener:     Option<TcpListener>,
  pub address:  SocketAddr,
  fronts:       TrieNode<Vec<HttpFront>>,
  pool:         Rc<RefCell<Pool<BufferQueue>>>,
  answers:      DefaultAnswers,
  config:       HttpListener,
  pub token:    Token,
  pub active:   bool,
}

pub struct ServerConfiguration {
  listeners:    HashMap<Token,Listener>,
  backends:     BackendMap,
  applications: HashMap<AppId, Application>,
  pool:         Rc<RefCell<Pool<BufferQueue>>>,
}

impl ServerConfiguration {
  pub fn new(pool: Rc<RefCell<Pool<BufferQueue>>>) -> ServerConfiguration {
      ServerConfiguration {
      listeners:    HashMap::new(),
      backends:     BackendMap::new(),
      applications: HashMap::new(),
      pool,
    }
  }

  pub fn add_listener(&mut self, config: HttpListener, pool: Rc<RefCell<Pool<BufferQueue>>>, token: Token) -> Option<Token> {
    if self.listeners.contains_key(&token) {
      None
    } else {
     let listener = Listener::new(config, pool, token);
      self.listeners.insert(listener.token.clone(), listener);
      Some(token)
    }
  }

  pub fn activate_listener(&mut self, event_loop: &mut Poll, addr: &SocketAddr, tcp_listener: Option<TcpListener>) -> Option<Token> {
    for listener in self.listeners.values_mut() {
      if &listener.address == addr {
        return listener.activate(event_loop, tcp_listener);
      }
    }
    None
  }

  pub fn give_back_listeners(&mut self) -> Vec<(SocketAddr, TcpListener)> {
    self.listeners.values_mut().filter(|l| l.listener.is_some()).map(|l| {
      (l.address.clone(), l.listener.take().unwrap())
    }).collect()
  }

  pub fn add_application(&mut self, application: Application, event_loop: &mut Poll) {
    let app_id = &application.app_id.clone();
    let lb_alg = application.load_balancing_policy;

    self.applications.insert(application.app_id.clone(), application);
    self.backends.set_load_balancing_policy_for_app(app_id, lb_alg);
  }

  pub fn remove_application(&mut self, app_id: &str, event_loop: &mut Poll) {
    self.applications.remove(app_id);
  }

  pub fn add_backend(&mut self, app_id: &str, backend: Backend, event_loop: &mut Poll) {
    self.backends.add_backend(app_id, backend);
  }

  pub fn remove_backend(&mut self, app_id: &str, backend_address: &SocketAddr, event_loop: &mut Poll) {
    self.backends.remove_backend(app_id, backend_address);
  }

  pub fn backend_from_app_id(&mut self, client: &mut Client, app_id: &str, front_should_stick: bool) -> Result<TcpStream,ConnectionError> {
    client.http().map(|h| h.set_app_id(String::from(app_id)));

    match self.backends.backend_from_app_id(app_id) {
      Err(e) => {
        Err(e)
      },
      Ok((backend, conn))  => {
        client.back_connected = BackendConnectionStatus::Connecting;
        if front_should_stick {
          let sticky_name =  self.listeners[&client.listen_token].config.sticky_name.clone();
          client.http().map(|http| {
            http.sticky_session =
              Some(StickySession::new(backend.borrow().sticky_id.clone().unwrap_or(backend.borrow().backend_id.clone())));
            http.sticky_name = sticky_name;
          });
        }
        client.metrics.backend_id = Some(backend.borrow().backend_id.clone());
        client.metrics.backend_start();
        client.backend = Some(backend);

        Ok(conn)
      }
    }
  }

  pub fn backend_from_sticky_session(&mut self, client: &mut Client, app_id: &str, sticky_session: String) -> Result<TcpStream,ConnectionError> {
    client.http().map(|h| h.set_app_id(String::from(app_id)));

    match self.backends.backend_from_sticky_session(app_id, &sticky_session) {
      Err(e) => {
        Err(e)
      },
      Ok((backend, conn))  => {
        client.back_connected = BackendConnectionStatus::Connecting;
        let sticky_name =  self.listeners[&client.listen_token].config.sticky_name.clone();
        client.http().map(|http| {
          http.sticky_session = Some(StickySession::new(backend.borrow().sticky_id.clone().unwrap_or(sticky_session.clone())));
          http.sticky_name = sticky_name;
        });
        client.metrics.backend_id = Some(backend.borrow().backend_id.clone());
        client.metrics.backend_start();
        client.backend = Some(backend);

        Ok(conn)
      }
    }
  }
}

impl Listener {
  pub fn new(config: HttpListener, pool: Rc<RefCell<Pool<BufferQueue>>>, token: Token) -> Listener {

    let front = config.front;

    let default = DefaultAnswers {
      NotFound: Rc::new(Vec::from(config.answer_404.as_bytes())),
      ServiceUnavailable: Rc::new(Vec::from(config.answer_503.as_bytes())),
      BadRequest: Rc::new(Vec::from(
          &b"HTTP/1.1 400 Bad Request\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"[..]
        )),
    };

    Listener {
      listener: None,
      address: config.front,
      fronts:  TrieNode::root(),
      pool,
      answers: default,
      config,
      token,
      active: false,
    }
  }

  pub fn activate(&mut self, event_loop: &mut Poll, tcp_listener: Option<TcpListener>) -> Option<Token> {
    if self.active {
      return None;
    }

    let listener = tcp_listener.or_else(|| server_bind(&self.config.front).map_err(|e| {
      error!("could not create listener {:?}: {:?}", self.config.front, e);
    }).ok());

    if let Some(ref sock) = listener {
      event_loop.register(sock, self.token, Ready::readable(), PollOpt::edge());
    } else {
      return None;
    }

    self.listener = listener;
    self.active = true;
    Some(self.token)
  }

  pub fn add_http_front(&mut self, mut http_front: HttpFront, event_loop: &mut Poll) -> Result<(), String> {
    match ::idna::domain_to_ascii(&http_front.hostname) {
      Ok(hostname) => {
        http_front.hostname = hostname;
        let front2 = http_front.clone();
        let front3 = http_front.clone();
        if let Some((_, ref mut fronts)) = self.fronts.domain_lookup_mut(&http_front.hostname.clone().into_bytes()) {
            if !fronts.contains(&front2) {
              fronts.push(front2);
            }
        }

        // FIXME: check that http front port matches the listener's port
        // FIXME: separate the port and hostname, match the hostname separately

        if self.fronts.domain_lookup(&http_front.hostname.clone().into_bytes()).is_none() {
          self.fronts.domain_insert(http_front.hostname.into_bytes(), vec![front3]);
        }
        Ok(())
      },
      Err(_) => Err(format!("Couldn't parse hostname {} to ascii", http_front.hostname))
    }
  }

  pub fn remove_http_front(&mut self, mut front: HttpFront, event_loop: &mut Poll) -> Result<(), String> {
    debug!("removing http_front {:?}", front);
    match ::idna::domain_to_ascii(&front.hostname) {
      Ok(hostname) => {
        front.hostname = hostname;

        let should_delete = {
          let fronts_opt = self.fronts.domain_lookup_mut(front.hostname.as_bytes());

          if let Some((_, fronts)) = fronts_opt {
            fronts.retain(|f| f != &front);
          }

          fronts_opt.as_ref().map(|(_,fronts)| fronts.len() == 0).unwrap_or(false)
        };

        if should_delete {
          self.fronts.domain_remove(&front.hostname.into());
        }

        Ok(())
      },
      Err(_) => Err(format!("Couldn't parse hostname {} to ascii", front.hostname))
    }
  }

  pub fn frontend_from_request(&self, host: &str, uri: &str) -> Option<&HttpFront> {
    let host: &str = if let Ok((i, (hostname, port))) = hostname_and_port(host.as_bytes()) {
      if i != &b""[..] {
        error!("frontend_from_request: invalid remaining chars after hostname. Host: {}", host);
        return None;
      }

      /*if port == Some(&b"80"[..]) {
      // it is alright to call from_utf8_unchecked,
      // we already verified that there are only ascii
      // chars in there
        unsafe { from_utf8_unchecked(hostname) }
      } else {
        host
      }
      */
      unsafe { from_utf8_unchecked(hostname) }
    } else {
      error!("hostname parsing failed for: '{}'", host);
      return None;
    };

    if let Some((_, http_fronts)) = self.fronts.domain_lookup(host.as_bytes()) {
      let matching_fronts = http_fronts.iter().filter(|f| uri.starts_with(&f.path_begin)); // ToDo match on uri
      let mut front = None;

      for f in matching_fronts {
        if front.is_none() {
          front = Some(f);
        }

        if let Some(ff) = front {
          if f.path_begin.len() > ff.path_begin.len() {
            front = Some(f)
          }
        }
      }
      front
    } else {
      None
    }
  }

  fn accept(&mut self) -> Result<TcpStream, AcceptError> {

    if let Some(ref sock) = self.listener {
      sock.accept().map_err(|e| {
        match e.kind() {
          ErrorKind::WouldBlock => AcceptError::WouldBlock,
          other => {
            error!("accept() IO error: {:?}", e);
            AcceptError::IoError
          }
        }
      }).map(|(sock,_)| sock)
    } else {
      error!("cannot accept connections, no listening socket available");
      Err(AcceptError::IoError)
    }
  }
}

impl ProxyConfiguration<Client> for ServerConfiguration {
  fn connect_to_backend(&mut self, poll: &mut Poll, client: &mut Client, back_token: Token) -> Result<BackendConnectAction,ConnectionError> {
    let h = try!(client.http().unwrap().state.as_ref().unwrap().get_host().ok_or(ConnectionError::NoHostGiven));

    let host: &str = if let Ok((i, (hostname, port))) = hostname_and_port(h.as_bytes()) {
      if i != &b""[..] {
        error!("connect_to_backend: invalid remaining chars after hostname. Host: {}", h);
        let answer = self.listeners[&client.listen_token].answers.BadRequest.clone();
        client.set_answer(DefaultAnswerStatus::Answer400, answer);
        return Err(ConnectionError::InvalidHost);
      }

      //FIXME: we should check that the port is right too

      if port == Some(&b"80"[..]) {
      // it is alright to call from_utf8_unchecked,
      // we already verified that there are only ascii
      // chars in there
        unsafe { from_utf8_unchecked(hostname) }
      } else {
        &h
      }
    } else {
      error!("hostname parsing failed for: '{}'", h);
      let answer = self.listeners[&client.listen_token].answers.BadRequest.clone();
      client.set_answer(DefaultAnswerStatus::Answer400, answer);
      return Err(ConnectionError::InvalidHost);
    };

    let sticky_session = client.http().unwrap().state.as_ref().unwrap().get_request_sticky_session();

    //FIXME: too many unwraps here
    let rl     = try!(client.http().unwrap().state.as_ref().unwrap().get_request_line().ok_or(ConnectionError::NoRequestLineGiven));
    if let Some(app_id) = self.listeners[&client.listen_token].frontend_from_request(&host, &rl.uri).map(|ref front| front.app_id.clone()) {

      let front_should_redirect_https = self.applications.get(&app_id).map(|ref app| app.https_redirect).unwrap_or(false);
      if front_should_redirect_https {
        let answer = format!("HTTP/1.1 301 Moved Permanently\r\nContent-Length: 0\r\nLocation: https://{}{}\r\n\r\n", host, rl.uri);
        client.set_answer(DefaultAnswerStatus::Answer301, Rc::new(answer.into_bytes()));
        return Err(ConnectionError::HttpsRedirect);
      }

      let front_should_stick = self.applications.get(&app_id).map(|ref app| app.sticky_session).unwrap_or(false);
      let old_app_id = client.http().and_then(|ref http| http.app_id.clone());
      let old_back_token = client.back_token();

      if (client.http().map(|h| h.app_id.as_ref()).unwrap_or(None) == Some(&app_id)) && client.back_connected == BackendConnectionStatus::Connected {
        if client.backend.as_ref().map(|backend| {
          let ref backend = *backend.borrow();
          self.backends.has_backend(&app_id, backend)
        }).unwrap_or(false) {
          //matched on keepalive
          client.metrics.backend_id = client.backend.as_ref().map(|i| i.borrow().backend_id.clone());
          client.metrics.backend_start();
          return Ok(BackendConnectAction::Reuse);
        } else {
          if let Some(token) = client.back_token() {
            let addr = client.close_backend(token, poll);
            if let Some((app_id, address)) = addr {
              self.close_backend(app_id, &address);
            }
          }
        }
      }

      // circuit breaker
      if client.back_connected == BackendConnectionStatus::Connecting {
        client.backend.as_ref().map(|backend| {
          let ref mut backend = *backend.borrow_mut();
          backend.dec_connections();
          backend.failures += 1;

          let already_unavailable = backend.retry_policy.is_down();
          backend.retry_policy.fail();
          incr!("backend.connections.error");
          if !already_unavailable && backend.retry_policy.is_down() {
            incr!("backend.down");
          }
        });


        //manually close the connection here because the back token was removed elsewhere
        client.backend = None;
        client.back_connected = BackendConnectionStatus::NotConnected;
        client.back_readiness().map(|r| r.event = UnixReady::from(Ready::empty()));
        client.back_socket().as_ref().map(|sock| {
          poll.deregister(*sock);
          sock.shutdown(Shutdown::Both);
        });

        if client.connection_attempt == CONN_RETRIES {
          error!("{} max connection attempt reached", client.log_context());
          let answer = self.listeners[&client.listen_token].answers.ServiceUnavailable.clone();
          client.set_answer(DefaultAnswerStatus::Answer503, answer);
        }
      }

      client.app_id = Some(app_id.clone());

      let conn = match (front_should_stick, sticky_session) {
        (true, Some(session)) => self.backend_from_sticky_session(client, &app_id, session),
        _ => self.backend_from_app_id(client, &app_id, front_should_stick),
      };

      match conn {
        Ok(socket) => {
          let new_app_id = client.http().and_then(|ref http| http.app_id.clone());
          let replacing_connection = old_app_id.is_some() && old_app_id != new_app_id;

          if replacing_connection {
            if let Some(token) = client.back_token() {
              let addr = client.close_backend(token, poll);
              if let Some((app_id, address)) = addr {
                self.close_backend(app_id, &address);
              }
            }
          }

          // we still want to use the new socket
          client.back_readiness().map(|r| r.interest  = UnixReady::from(Ready::writable()));


          socket.set_nodelay(true);
          client.back_readiness().map(|r| {
            r.interest.insert(Ready::writable());
            r.interest.insert(UnixReady::hup());
            r.interest.insert(UnixReady::error());
          });
          if replacing_connection {
            client.set_back_token(old_back_token.expect("FIXME"));
            poll.register(
              &socket,
              client.back_token().expect("FIXME"),
              Ready::readable() | Ready::writable() | Ready::from(UnixReady::hup() | UnixReady::error()),
              PollOpt::edge()
            );
            client.set_back_socket(socket);
            Ok(BackendConnectAction::Replace)
          } else {
            poll.register(
              &socket,
              back_token,
              Ready::readable() | Ready::writable() | Ready::from(UnixReady::hup() | UnixReady::error()),
              PollOpt::edge()
            );

            client.set_back_socket(socket);
            client.set_back_token(back_token);
            Ok(BackendConnectAction::New)
          }
          //Ok(())
        },
        Err(ConnectionError::NoBackendAvailable) => {
          let answer =  self.listeners[&client.listen_token].answers.ServiceUnavailable.clone();
          client.set_answer(DefaultAnswerStatus::Answer503, answer);
          Err(ConnectionError::NoBackendAvailable)
        }
        Err(ConnectionError::HostNotFound) => {
          let answer = self.listeners[&client.listen_token].answers.NotFound.clone();
          client.set_answer(DefaultAnswerStatus::Answer404, answer);
          Err(ConnectionError::HostNotFound)
        }
        e => panic!(e)
      }
    } else {
      let answer = self.listeners[&client.listen_token].answers.NotFound.clone();
      client.set_answer(DefaultAnswerStatus::Answer404, answer);
      Err(ConnectionError::HostNotFound)
    }
  }

  fn notify(&mut self, event_loop: &mut Poll, message: OrderMessage) -> OrderMessageAnswer {
    // ToDo temporary
    //trace!("{} notified", message);
    match message.order {
      Order::AddApplication(application) => {
        debug!("{} add application {:?}", message.id, application);
        self.add_application(application, event_loop);
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None }
      },
      Order::RemoveApplication(application) => {
        debug!("{} remove application {:?}", message.id, application);
        self.remove_application(&application, event_loop);
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None }
      },
      Order::AddHttpFront(front) => {
        debug!("{} add front {:?}", message.id, front);
        if let Some(mut listener) = self.listeners.values_mut().find(|l| l.address == front.address) {
          match listener.add_http_front(front, event_loop) {
            Ok(_) => OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None },
            Err(err) => OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Error(err), data: None }
          }
        } else {
          panic!("no HTTP listener found for front: {:?}", front);

          //let (listener, tokens) = Listener::new(HttpListener::default(), event_loop,
          //  self.pool.clone(), None, token: Token) -> (Listener,HashSet<Token>
        }
      },
      Order::RemoveHttpFront(front) => {
        debug!("{} front {:?}", message.id, front);
        if let Some(listener) = self.listeners.values_mut().find(|l| l.address == front.address) {
          match (*listener).remove_http_front(front, event_loop) {
            Ok(_) => OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None },
            Err(err) => OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Error(err), data: None }
          }
        } else {
          panic!("trying to remove front from non existing listener");
        }
      },
      Order::AddBackend(backend) => {
        debug!("{} add backend {:?}", message.id, backend);
        let new_backend = Backend::new(&backend.backend_id, backend.address.clone(), backend.sticky_id.clone(), backend.load_balancing_parameters, backend.backup);
        self.add_backend(&backend.app_id, new_backend, event_loop);
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None }
      },
      Order::RemoveBackend(backend) => {
        debug!("{} remove backend {:?}", message.id, backend);
        self.remove_backend(&backend.app_id, &backend.address, event_loop);
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None }
      },
      Order::RemoveListener(remove) => {
        info!("removing http listener at address {:?}", remove.front);
        fixme!();
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Error(String::from("unimplemented")), data: None }
      },
      Order::SoftStop => {
        info!("{} processing soft shutdown", message.id);
        self.listeners.drain().map(|(token, mut l)| {
          l.listener.take().map(|sock| {
            event_loop.deregister(&sock);
          })
        });
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Processing, data: None }
      },
      Order::HardStop => {
        info!("{} hard shutdown", message.id);
        self.listeners.drain().map(|(token, mut l)| {
          l.listener.take().map(|sock| {
            event_loop.deregister(&sock);
          })
        });
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Processing, data: None }
      },
      Order::Status => {
        debug!("{} status", message.id);
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None }
      },
      Order::Logging(logging_filter) => {
        info!("{} changing logging filter to {}", message.id, logging_filter);
        logging::LOGGER.with(|l| {
          let directives = logging::parse_logging_spec(&logging_filter);
          l.borrow_mut().set_directives(directives);
        });
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Ok, data: None }
      },
      command => {
        debug!("{} unsupported message, ignoring: {:?}", message.id, command);
        OrderMessageAnswer{ id: message.id, status: OrderMessageStatus::Error(String::from("unsupported message")), data: None }
      }
    }
  }

  fn accept(&mut self, token: ListenToken) -> Result<TcpStream, AcceptError> {
    self.listeners.get_mut(&Token(token.0)).unwrap().accept()
  }

  fn create_client(&mut self, frontend_sock: TcpStream, listen_token: ListenToken, poll: &mut Poll, client_token: Token, timeout: Timeout)
  -> Result<(Rc<RefCell<Client>>, bool), AcceptError> {
    if let Some(ref listener) = self.listeners.get(&Token(listen_token.0)) {
      frontend_sock.set_nodelay(true);
      if let Some(c) = Client::new(frontend_sock, client_token, Rc::downgrade(&self.pool),
      listener.config.public_address, listener.config.expect_proxy, listener.config.sticky_name.clone(), timeout,
      listener.token) {
        poll.register(
          c.front_socket(),
          client_token,
          Ready::readable() | Ready::writable() | Ready::from(UnixReady::hup() | UnixReady::error()),
          PollOpt::edge()
          );

        Ok((Rc::new(RefCell::new(c)), false))
      } else {
        Err(AcceptError::TooManyClients)
      }
    } else {
      //FIXME
      Err(AcceptError::IoError)
    }

  }

  fn close_backend(&mut self, app_id: String, addr: &SocketAddr) {
    self.backends.close_backend_connection(&app_id, &addr);
  }

  fn listen_port_state(&self, port: &u16) -> ListenPortState {
    //FIXME TOKEN
    fixme!();
    ListenPortState::Available
    //let token = Token(0);
    //if port == &self.listeners[&token].address.port() { ListenPortState::InUse } else { ListenPortState::Available }
  }
}

pub struct InitialServerConfiguration {
  backends:        BackendMap,
  fronts:          TrieNode<Vec<HttpFront>>,
  applications:    HashMap<AppId, Application>,
  answers:         DefaultAnswers,
  config:          HttpListener,
}

impl InitialServerConfiguration {
  pub fn new(config: HttpListener, state: &ConfigState) -> InitialServerConfiguration {

    let answers = DefaultAnswers {
      NotFound: Rc::new(Vec::from(config.answer_404.as_bytes())),
      ServiceUnavailable: Rc::new(Vec::from(config.answer_503.as_bytes())),
      BadRequest: Rc::new(Vec::from(
        &b"HTTP/1.1 400 Bad Request\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"[..]
      )),
    };

    let mut backends = BackendMap::new();
    backends.import_configuration_state(&state.backends);

    let mut fronts = TrieNode::root();

    for (domain, http_fronts) in state.http_fronts.iter() {
      fronts.domain_insert(domain.clone().into_bytes(), http_fronts.clone());
    }

    InitialServerConfiguration {
      backends:     backends,
      fronts:       fronts,
      applications: state.applications.clone(),
      answers:      answers,
      config:       config,
    }
  }

  pub fn start_listening(self, event_loop: &mut Poll, start_at:usize, pool: Rc<RefCell<Pool<BufferQueue>>>) -> io::Result<ServerConfiguration> {

    unimplemented!()
/*
    let front = self.config.front;
    match server_bind(&front) {
      Ok(sock) => {
        event_loop.register(&sock, Token(start_at), Ready::readable(), PollOpt::edge());

        Ok(ServerConfiguration {
          listener:      Some(sock),
          address:       self.config.front,
          applications:  self.applications,
          backends:      self.backends,
          fronts:        self.fronts,
          pool:          pool,
          answers:       self.answers,
          config:        self.config,
        })
      },
      Err(e) => {
        let formatted_err = format!("could not create listener {:?}: {:?}", front, e);
        error!("{}", formatted_err);
        Err(e)
      }
    }

*/
  }
}


pub fn start(config: HttpListener, channel: ProxyChannel, max_buffers: usize, buffer_size: usize) {
  use network::proxy::ProxyClientCast;
  let mut event_loop  = Poll::new().expect("could not create event loop");
  let max_listeners   = 1;

  let pool = Rc::new(RefCell::new(
    Pool::with_capacity(2*max_buffers, 0, || BufferQueue::with_capacity(buffer_size))
  ));
  let mut clients: Slab<Rc<RefCell<ProxyClientCast>>,ClientToken> = Slab::with_capacity(max_buffers);
  {
    let entry = clients.vacant_entry().expect("client list should have enough room at startup");
    info!("taking token {:?} for channel", entry.index());
    entry.insert(Rc::new(RefCell::new(ListenClient { protocol: Protocol::HTTPListen })));
  }
  {
    let entry = clients.vacant_entry().expect("client list should have enough room at startup");
    info!("taking token {:?} for timer", entry.index());
    entry.insert(Rc::new(RefCell::new(ListenClient { protocol: Protocol::HTTPListen })));
  }
  {
    let entry = clients.vacant_entry().expect("client list should have enough room at startup");
    info!("taking token {:?} for metrics", entry.index());
    entry.insert(Rc::new(RefCell::new(ListenClient { protocol: Protocol::HTTPListen })));
  }

  let token = {
    let entry = clients.vacant_entry().expect("client list should have enough room at startup");
    let e = entry.insert(Rc::new(RefCell::new(ListenClient { protocol: Protocol::HTTPListen })));
    Token(e.index().0)
  };

  let front = config.front.clone();
  let mut configuration = ServerConfiguration::new(pool.clone());
  let _ = configuration.add_listener(config, pool.clone(), token);
  let _ = configuration.activate_listener(&mut event_loop, &front, None);
  let (scm_server, scm_client) = UnixStream::pair().unwrap();
  let scm = ScmSocket::new(scm_client.into_raw_fd());
  scm.send_listeners(Listeners {
    http: Vec::new(),
    tls:  Vec::new(),
    tcp:  Vec::new(),
  });

  let mut server    = Server::new(event_loop, channel, ScmSocket::new(scm_server.into_raw_fd()),
    clients, pool, Some(configuration), None, None, None, max_buffers, 60, 1800, 60);

  println!("starting event loop");
  server.run();
  println!("ending event loop");
}

#[cfg(test)]
mod tests {
  extern crate tiny_http;
  use super::*;
  use slab::Slab;
  use mio::Poll;
  use std::collections::HashMap;
  use std::net::{TcpListener, TcpStream, Shutdown};
  use std::io::{Read,Write};
  use std::{thread,str};
  use std::sync::{
    Arc, Barrier,
    mpsc::channel
  };
  use std::net::SocketAddr;
  use std::str::FromStr;
  use std::time::Duration;
  use sozu_command::messages::{Order,HttpFront,Backend,HttpListener,OrderMessage,OrderMessageAnswer, LoadBalancingParams};
  use network::buffer_queue::BufferQueue;
  use network::pool::Pool;
  use sozu_command::config::LoadBalancingAlgorithms;

  #[allow(unused_mut, unused_must_use, unused_variables)]
  #[test]
  fn mi() {
    setup_test_logger!();
    let barrier = Arc::new(Barrier::new(2));
    start_server(1025, barrier.clone());
    barrier.wait();

    let front: SocketAddr = FromStr::from_str("127.0.0.1:1024").expect("could not parse address");
    let config = HttpListener {
      front: front,
      ..Default::default()
    };

    let (mut command, channel) = Channel::generate(1000, 10000).expect("should create a channel");
    let jg = thread::spawn(move || {
      setup_test_logger!();
      start(config, channel, 10, 16384);
    });

    let front = HttpFront { app_id: String::from("app_1"), address: "127.0.0.1:1024".parse().unwrap(), hostname: String::from("localhost"), path_begin: String::from("/") };
    command.write_message(&OrderMessage { id: String::from("ID_ABCD"), order: Order::AddHttpFront(front) });
    let backend = Backend { app_id: String::from("app_1"),backend_id: String::from("app_1-0"), address: "127.0.0.1:1025".parse().unwrap(), load_balancing_parameters: Some(LoadBalancingParams::default()), sticky_id: None, backup: None };
    command.write_message(&OrderMessage { id: String::from("ID_EFGH"), order: Order::AddBackend(backend) });

    println!("test received: {:?}", command.read_message());
    println!("test received: {:?}", command.read_message());

    let mut client = TcpStream::connect(("127.0.0.1", 1024)).expect("could not parse address");

    // 5 seconds of timeout
    client.set_read_timeout(Some(Duration::new(5,0)));
    let mut w  = client.write(&b"GET / HTTP/1.1\r\nHost: localhost:1024\r\nConnection: Close\r\n\r\n"[..]);
    println!("http client write: {:?}", w);

    barrier.wait();
    let mut buffer = [0;4096];
    let mut index = 0;

    loop {
      assert!(index <= 201);
      if index == 201 {
        break;
      }

      let mut r = client.read(&mut buffer[index..]);
      println!("http client read: {:?}", r);
      match r {
        Err(e)      => assert!(false, "client request should not fail. Error: {:?}",e),
        Ok(sz) => {
          index += sz;
        }
      }
    }
    println!("Response: {}", str::from_utf8(&buffer[..index]).expect("could not make string from buffer"));
  }

  #[allow(unused_mut, unused_must_use, unused_variables)]
  #[test]
  fn keep_alive() {
    setup_test_logger!();
    let barrier = Arc::new(Barrier::new(2));
    start_server(1028, barrier.clone());
    barrier.wait();

    let front: SocketAddr = FromStr::from_str("127.0.0.1:1031").expect("could not parse address");
    let config = HttpListener {
      front: front,
      ..Default::default()
    };

    let (mut command, channel) = Channel::generate(1000, 10000).expect("should create a channel");

    let jg = thread::spawn(move|| {
      start(config, channel, 10, 16384);
    });

    let front = HttpFront { app_id: String::from("app_1"), address: "127.0.0.1:1031".parse().unwrap(), hostname: String::from("localhost"), path_begin: String::from("/") };
    command.write_message(&OrderMessage { id: String::from("ID_ABCD"), order: Order::AddHttpFront(front) });
    let backend = Backend { app_id: String::from("app_1"), backend_id: String::from("app_1-0"), address: "127.0.0.1:1028".parse().unwrap(), load_balancing_parameters: Some(LoadBalancingParams::default()), sticky_id: None, backup: None };
    command.write_message(&OrderMessage { id: String::from("ID_EFGH"), order: Order::AddBackend(backend) });

    println!("test received: {:?}", command.read_message());
    println!("test received: {:?}", command.read_message());

    let mut client = TcpStream::connect(("127.0.0.1", 1031)).expect("could not parse address");
    // 5 seconds of timeout
    client.set_read_timeout(Some(Duration::new(5,0)));

    let mut w  = client.write(&b"GET / HTTP/1.1\r\nHost: localhost:1031\r\n\r\n"[..]);
    println!("http client write: {:?}", w);
    barrier.wait();

    let mut buffer = [0;4096];
    let mut index = 0;

    loop {
      assert!(index <= 201);
      if index == 201 {
        break;
      }

      let mut r = client.read(&mut buffer[index..]);
      println!("http client read: {:?}", r);
      match r {
        Err(e)      => assert!(false, "client request should not fail. Error: {:?}",e),
        Ok(sz) => {
          index += sz;
        }
      }
    }
    println!("Response: {}", str::from_utf8(&buffer[..index]).expect("could not make string from buffer"));

    println!("first request ended, will send second one");
    let mut w2  = client.write(&b"GET / HTTP/1.1\r\nHost: localhost:1031\r\n\r\n"[..]);
    println!("http client write: {:?}", w2);
    barrier.wait();

    let mut buffer2 = [0;4096];
    let mut index = 0;

    loop {
      assert!(index <= 201);
      if index == 201 {
        break;
      }

      let mut r2 = client.read(&mut buffer2[index..]);
      println!("http client read: {:?}", r2);
      match r2 {
        Err(e)      => assert!(false, "client request should not fail. Error: {:?}",e),
        Ok(sz) => {
          index += sz;
        }
      }
    }
    println!("Response: {}", str::from_utf8(&buffer2[..index]).expect("could not make string from buffer"));
  }

  #[allow(unused_mut, unused_must_use, unused_variables)]
  #[test]
  fn https_redirect() {
    setup_test_logger!();
    let front: SocketAddr = FromStr::from_str("127.0.0.1:1041").expect("could not parse address");
    let config = HttpListener {
      front: front,
      ..Default::default()
    };

    let (mut command, channel) = Channel::generate(1000, 10000).expect("should create a channel");
    let jg = thread::spawn(move || {
      setup_test_logger!();
      start(config, channel, 10, 16384);
    });

    let application = Application { app_id: String::from("app_1"), sticky_session: false, https_redirect: true, proxy_protocol: None, load_balancing_policy: LoadBalancingAlgorithms::default() };
    command.write_message(&OrderMessage { id: String::from("ID_ABCD"), order: Order::AddApplication(application) });
    let front = HttpFront { app_id: String::from("app_1"), address: "127.0.0.1:1041".parse().unwrap(), hostname: String::from("localhost"), path_begin: String::from("/") };
    command.write_message(&OrderMessage { id: String::from("ID_EFGH"), order: Order::AddHttpFront(front) });
    let backend = Backend { app_id: String::from("app_1"),backend_id: String::from("app_1-0"), address: "127.0.0.1:1040".parse().unwrap(), load_balancing_parameters: Some(LoadBalancingParams::default()), sticky_id: None, backup: None };
    command.write_message(&OrderMessage { id: String::from("ID_IJKL"), order: Order::AddBackend(backend) });

    println!("test received: {:?}", command.read_message());
    println!("test received: {:?}", command.read_message());
    println!("test received: {:?}", command.read_message());

    let mut client = TcpStream::connect(("127.0.0.1", 1041)).expect("could not parse address");
    // 5 seconds of timeout
    client.set_read_timeout(Some(Duration::new(5,0)));

    let mut w  = client.write(&b"GET /redirected?true HTTP/1.1\r\nHost: localhost\r\nConnection: Close\r\n\r\n"[..]);
    println!("http client write: {:?}", w);

    let expected_answer = "HTTP/1.1 301 Moved Permanently\r\nContent-Length: 0\r\nLocation: https://localhost/redirected?true\r\n\r\n";
    let mut buffer = [0;4096];
    let mut index = 0;
    loop {
      assert!(index <= expected_answer.len());
      if index == expected_answer.len() {
        break;
      }

      let mut r = client.read(&mut buffer[..]);
      println!("http client read: {:?}", r);
      match r {
        Err(e)      => assert!(false, "client request should not fail. Error: {:?}",e),
        Ok(sz) => {
          index += sz;
        }
      }
    }

    let answer = str::from_utf8(&buffer[..index]).expect("could not make string from buffer");
    println!("Response: {}", answer);
    assert_eq!(answer, expected_answer);
  }


  use self::tiny_http::{Server, Response};

  #[allow(unused_mut, unused_must_use, unused_variables)]
  fn start_server(port: u16, barrier: Arc<Barrier>) {
    thread::spawn(move|| {
      let server = Server::http(&format!("127.0.0.1:{}", port)).expect("could not create server");
      info!("starting web server in port {}", port);
      barrier.wait();

      for request in server.incoming_requests() {
        println!("backend web server got request -> method: {:?}, url: {:?}, headers: {:?}",
          request.method(),
          request.url(),
          request.headers()
        );

        let response = Response::from_string("hello world");
        request.respond(response);
        println!("backend web server sent response");
        barrier.wait();
        println!("server session stopped");
      }

      println!("server on port {} closed", port);
    });
  }

  use mio::net;
  #[test]
  fn frontend_from_request_test() {
    let app_id1 = "app_1".to_owned();
    let app_id2 = "app_2".to_owned();
    let app_id3 = "app_3".to_owned();
    let uri1 = "/".to_owned();
    let uri2 = "/yolo".to_owned();
    let uri3 = "/yolo/swag".to_owned();

    let mut fronts = TrieNode::root();
    fronts.domain_insert(Vec::from(&b"lolcatho.st"[..]), vec![
      HttpFront { app_id: app_id1, address: "0.0.0.0:80".parse().unwrap(), hostname: "lolcatho.st".to_owned(), path_begin: uri1 },
      HttpFront { app_id: app_id2, address: "0.0.0.0:80".parse().unwrap(), hostname: "lolcatho.st".to_owned(), path_begin: uri2 },
      HttpFront { app_id: app_id3, address: "0.0.0.0:80".parse().unwrap(), hostname: "lolcatho.st".to_owned(), path_begin: uri3 }
    ]);
    fronts.domain_insert(Vec::from(&b"other.domain"[..]), vec![
      HttpFront { app_id: "app_1".to_owned(), address: "0.0.0.0:80".parse().unwrap(), hostname: "other.domain".to_owned(), path_begin: "/test".to_owned() },
    ]);

    let front: SocketAddr = FromStr::from_str("127.0.0.1:1030").expect("could not parse address");
    let listener = Listener {
      listener: None,
      address:  front,
      fronts,
      pool:      Rc::new(RefCell::new(Pool::with_capacity(1,0, || BufferQueue::with_capacity(16384)))),
      answers:   DefaultAnswers {
        NotFound: Rc::new(Vec::from(&b"HTTP/1.1 404 Not Found\r\n\r\n"[..])),
        ServiceUnavailable: Rc::new(Vec::from(&b"HTTP/1.1 503 your application is in deployment\r\n\r\n"[..])),
        BadRequest: Rc::new(Vec::from(
          &b"HTTP/1.1 400 Bad Request\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n"[..]
        )),
      },
      config: Default::default(),
      token: Token(0),
      active: true,
    };

    let frontend1 = listener.frontend_from_request("lolcatho.st", "/");
    let frontend2 = listener.frontend_from_request("lolcatho.st", "/test");
    let frontend3 = listener.frontend_from_request("lolcatho.st", "/yolo/test");
    let frontend4 = listener.frontend_from_request("lolcatho.st", "/yolo/swag");
    let frontend5 = listener.frontend_from_request("domain", "/");
    assert_eq!(frontend1.expect("should find frontend").app_id, "app_1");
    assert_eq!(frontend2.expect("should find frontend").app_id, "app_1");
    assert_eq!(frontend3.expect("should find frontend").app_id, "app_2");
    assert_eq!(frontend4.expect("should find frontend").app_id, "app_3");
    assert_eq!(frontend5, None);
  }
}
