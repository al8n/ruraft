use std::{
  io,
  net::{IpAddr, SocketAddr, ToSocketAddrs},
  path::PathBuf,
  str::FromStr,
};

use agnostic::{
  net::dns::{AsyncConnectionProvider, Dns},
  Runtime,
};
use crossbeam_skiplist::SkipMap;
use ruraft_core::transport::NodeAddressResolver;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(PartialEq, Eq, Hash, Clone)]
enum Kind {
  Ip(IpAddr),
  Domain { safe: SmolStr, original: SmolStr },
}

/// An error which can be returned when parsing a [`NodeAddress`].
#[derive(Debug, thiserror::Error)]
pub enum ParseNodeAddressError {
  #[error("address is missing port")]
  MissingPort,
  #[error("invalid domain")]
  InvalidDomain,
  #[error("invalid port: {0}")]
  InvalidPort(#[from] std::num::ParseIntError),
}

/// A node address which supports both `domain:port` and socket address.
///
/// e.g. Valid format
/// 1. `www.example.com:8080`
/// 2. `[::1]:8080`
/// 3. `127.0.0.1:8080`
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct NodeAddress {
  kind: Kind,
  port: u16,
}

impl Serialize for NodeAddress {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    match &self.kind {
      Kind::Ip(ip) => SocketAddr::new(*ip, self.port)
        .to_string()
        .serialize(serializer),
      Kind::Domain { original, .. } => serializer.serialize_str(original.as_str()),
    }
  }
}

impl<'de> Deserialize<'de> for NodeAddress {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    <&str as Deserialize>::deserialize(deserializer)
      .and_then(|s| NodeAddress::from_str(s).map_err(<D::Error as serde::de::Error>::custom))
  }
}

impl From<SocketAddr> for NodeAddress {
  fn from(addr: SocketAddr) -> Self {
    Self {
      kind: Kind::Ip(addr.ip()),
      port: addr.port(),
    }
  }
}

impl From<(IpAddr, u16)> for NodeAddress {
  fn from(addr: (IpAddr, u16)) -> Self {
    Self {
      kind: Kind::Ip(addr.0),
      port: addr.1,
    }
  }
}

impl TryFrom<String> for NodeAddress {
  type Error = ParseNodeAddressError;

  fn try_from(s: String) -> Result<Self, Self::Error> {
    NodeAddress::from_str(s.as_str())
  }
}

impl TryFrom<&str> for NodeAddress {
  type Error = ParseNodeAddressError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    NodeAddress::from_str(value)
  }
}

impl FromStr for NodeAddress {
  type Err = ParseNodeAddressError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let res: Result<SocketAddr, _> = s.parse();
    match res {
      Ok(addr) => Ok(addr.into()),
      Err(_) => {
        let res: Result<IpAddr, _> = s.parse();
        match res {
          Ok(_) => Err(ParseNodeAddressError::MissingPort),
          Err(_) => {
            let Some((domain, port)) = s.rsplit_once(':') else {
              return Err(ParseNodeAddressError::MissingPort);
            };

            let port = port.parse().map_err(ParseNodeAddressError::InvalidPort)?;
            idna::domain_to_ascii_strict(domain)
              .map(|mut domain| {
                // make sure we will only issue one query
                if !domain.ends_with('.') {
                  domain.push('.');
                }

                Self {
                  kind: Kind::Domain {
                    safe: SmolStr::from(domain),
                    original: SmolStr::from(s),
                  },
                  port,
                }
              })
              .map_err(|_| ParseNodeAddressError::InvalidDomain)
          }
        }
      }
    }
  }
}

impl core::fmt::Display for NodeAddress {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match &self.kind {
      Kind::Ip(addr) => write!(f, "{}", SocketAddr::new(*addr, self.port)),
      Kind::Domain { original, .. } => write!(f, "{original}:{}", self.port),
    }
  }
}

impl NodeAddress {
  /// Returns the domain of the address if this address can only be represented by domain name
  pub fn domain(&self) -> Option<&str> {
    match &self.kind {
      Kind::Ip(_) => None,
      Kind::Domain { original, .. } => Some(original.rsplit_once(':').unwrap().1),
    }
  }

  /// Returns the ip of the address if this address can be represented by [`IpAddr`]
  pub const fn ip(&self) -> Option<IpAddr> {
    match &self.kind {
      Kind::Ip(addr) => Some(*addr),
      Kind::Domain { .. } => None,
    }
  }

  /// Returns the port
  pub const fn port(&self) -> u16 {
    self.port
  }

  /// Set the port
  pub fn set_port(&mut self, port: u16) {
    self.port = port;
  }
}

#[derive(Debug, thiserror::Error)]
enum ResolveErrorKind {
  #[error("cannot resolve an ip address for {0}")]
  NotFound(SmolStr),
  #[error("{0}")]
  Resolve(#[from] trust_dns_resolver::error::ResolveError),
}

/// The error type for errors that get returned when resolving fails
#[derive(Debug)]
#[repr(transparent)]
pub struct ResolveError(ResolveErrorKind);

impl core::fmt::Display for ResolveError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    self.0.fmt(f)
  }
}

impl std::error::Error for ResolveError {}

impl From<ResolveErrorKind> for ResolveError {
  fn from(value: ResolveErrorKind) -> Self {
    Self(value)
  }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("{0}")]
  IO(#[from] io::Error),
  #[error("{0}")]
  Resolve(#[from] ResolveError),
}

/// The options used to construct a [`DnsResolver`].
#[derive(Debug, Clone)]
pub struct DnsResolverOptions {
  dns_config_path: Option<PathBuf>,
}

impl DnsResolverOptions {
  /// Create a new [`DnsResolverOptions`] with the default DNS configuration path.
  ///
  /// Default DNS configuration path:
  /// 1. `Some(PathBuf::from("/etc/resolv.conf"))` for UNIX
  /// 2. `None` on other OS
  pub fn new() -> Self {
    Self::default()
  }

  /// Set the default dns configuration file path in builder pattern
  pub fn with_dns_config(mut self, p: Option<PathBuf>) -> Self {
    self.dns_config_path = p;
    self
  }

  /// Set the default dns configuration file path
  pub fn set_dns_config(&mut self, p: Option<PathBuf>) {
    self.dns_config_path = p;
  }

  /// Returns the default dns configuration file path, if any.
  pub fn dns_config(&self) -> Option<&PathBuf> {
    self.dns_config_path.as_ref()
  }
}

#[cfg(unix)]
impl Default for DnsResolverOptions {
  fn default() -> Self {
    Self {
      dns_config_path: Some(PathBuf::from("/etc/resolv.conf")),
    }
  }
}

#[cfg(not(unix))]
impl Default for DnsResolverOptions {
  fn default() -> Self {
    Self {
      dns_config_path: None,
    }
  }
}

/// A resolver which supports both `domain:port` and socket address.
/// If you can make sure, you always play with [`SocketAddr`], you may want to
/// use [`SocketAddrResolver`](crate::transport::resolver::default::SocketAddrResolver).
///
/// e.g. valid address format:
/// 1. `www.example.com:8080` // domain
/// 2. `[::1]:8080` // ipv6
/// 3. `127.0.0.1:8080` // ipv4
pub struct DnsResolver<R: Runtime> {
  dns: Option<Dns<R>>,
  cache: SkipMap<SmolStr, SocketAddr>,
}

impl<R: Runtime> DnsResolver<R> {
  pub fn new(opts: DnsResolverOptions) -> Result<Self, Error> {
    let dns = if let Some(path) = opts.dns_config_path {
      let (config, options) = dns_util::read_resolv_conf(&path)?;
      if config.name_servers().is_empty() {
        tracing::warn!(
          target = "ruraft.resolver.dns",
          "no DNS servers found in {}",
          path.display()
        );

        None
      } else {
        Some(Dns::new(config, options, AsyncConnectionProvider::new()))
      }
    } else {
      tracing::warn!(
        target = "ruraft.resolver.dns",
        "no default DNS configuration file",
      );
      None
    };

    Ok(Self {
      dns,
      cache: Default::default(),
    })
  }
}

#[async_trait::async_trait]
impl<R: Runtime> NodeAddressResolver for DnsResolver<R> {
  type NodeAddress = NodeAddress;
  type Error = Error;
  type Runtime = R;

  async fn resolve(&self, address: &Self::NodeAddress) -> Result<SocketAddr, Self::Error> {
    match &address.kind {
      Kind::Ip(ip) => Ok(SocketAddr::new(*ip, address.port)),
      Kind::Domain { safe, original } => {
        // First, check cache
        if let Some(ent) = self.cache.get(safe) {
          return Ok(*ent.value());
        }

        // Second, TCP lookup ip address
        if let Some(ref dns) = self.dns {
          if let Some(ip) = dns
            .lookup_ip(safe.as_str())
            .await
            .map_err(|e| ResolveError::from(ResolveErrorKind::from(e)))?
            .into_iter()
            .next()
          {
            let addr = SocketAddr::new(ip, address.port);
            self.cache.insert(safe.clone(), addr);
            return Ok(addr);
          }
        }

        // Finally, try to find the socket addr locally
        let res = ToSocketAddrs::to_socket_addrs(&(safe.as_str(), address.port))?;

        if let Some(addr) = res.into_iter().next() {
          self.cache.insert(safe.clone(), addr);
          return Ok(addr);
        }

        Err(Error::Resolve(ResolveError(ResolveErrorKind::NotFound(
          original.clone(),
        ))))
      }
    }
  }
}

#[cfg(not(target_family = "wasm"))]
mod dns_util {
  use std::{io, path::Path};

  use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};

  pub(crate) fn read_resolv_conf<P: AsRef<Path>>(
    path: P,
  ) -> io::Result<(ResolverConfig, ResolverOpts)> {
    std::fs::read_to_string(path).and_then(trust_dns_resolver::system_conf::parse_resolv_conf)
  }
}

#[cfg(target_family = "wasm")]
mod dns_util {
  use std::{
    fs::File,
    io::{self, Read},
    net::SocketAddr,
    path::Path,
    time::Duration,
  };
  use trust_dns_resolver::{
    config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts},
    Name,
  };

  const DEFAULT_PORT: u16 = 53;

  pub(crate) fn read_resolv_conf<P: AsRef<Path>>(
    path: P,
  ) -> io::Result<(ResolverConfig, ResolverOpts)> {
    let mut data = String::new();
    let mut file = File::open(path)?;
    file.read_to_string(&mut data)?;
    parse_resolv_conf(&data)
  }

  fn parse_resolv_conf<T: AsRef<[u8]>>(data: T) -> io::Result<(ResolverConfig, ResolverOpts)> {
    let parsed_conf = resolv_conf::Config::parse(&data).map_err(|e| {
      io::Error::new(
        io::ErrorKind::Other,
        format!("Error parsing resolv.conf: {e}"),
      )
    })?;
    into_resolver_config(parsed_conf)
  }

  fn into_resolver_config(
    parsed_config: resolv_conf::Config,
  ) -> io::Result<(ResolverConfig, ResolverOpts)> {
    let domain = None;

    // nameservers
    let mut nameservers = Vec::<NameServerConfig>::with_capacity(parsed_config.nameservers.len());
    for ip in &parsed_config.nameservers {
      nameservers.push(NameServerConfig {
        socket_addr: SocketAddr::new(ip.into(), DEFAULT_PORT),
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_negative_responses: false,
        #[cfg(feature = "dns-over-rustls")]
        tls_config: None,
        bind_addr: None,
      });
      nameservers.push(NameServerConfig {
        socket_addr: SocketAddr::new(ip.into(), DEFAULT_PORT),
        protocol: Protocol::Tcp,
        tls_dns_name: None,
        trust_negative_responses: false,
        #[cfg(feature = "dns-over-rustls")]
        tls_config: None,
        bind_addr: None,
      });
    }
    if nameservers.is_empty() {
      tracing::warn!("no nameservers found in config");
    }

    // search
    let mut search = vec![];
    for search_domain in parsed_config.get_last_search_or_domain() {
      search.push(Name::from_str_relaxed(search_domain).map_err(|e| {
        io::Error::new(
          io::ErrorKind::Other,
          format!("Error parsing resolv.conf: {e}"),
        )
      })?);
    }

    let config = ResolverConfig::from_parts(domain, search, nameservers);

    let mut options = ResolverOpts::default();
    options.timeout = Duration::from_secs(parsed_config.timeout as u64);
    options.attempts = parsed_config.attempts as usize;
    options.ndots = parsed_config.ndots as usize;

    Ok((config, options))
  }
}

#[test]
fn test() {}
