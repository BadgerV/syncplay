use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};

use crate::error::Result;
use crate::state::shared::{DiscoveredSender, SharedApp, MDNS_SERVICE_TYPE};

/// Manages mDNS registration (sender) or browsing (receiver).
pub struct DiscoveryService {
    daemon: ServiceDaemon,
}

impl DiscoveryService {
    /// Create a new discovery service backed by the mdns-sd daemon.
    pub fn new() -> Result<Self> {
        let daemon = ServiceDaemon::new().map_err(|e| {
            crate::error::SyncPlayError::Mdns(format!("Failed to create mDNS daemon: {e}"))
        })?;
        Ok(Self { daemon })
    }

    /// Register this machine as a SyncPlay sender.
    ///
    /// `ip` is this host's LAN address. mdns-sd requires the *hostname* (A/AAAA
    /// record name) to end in `.local.` and treats it separately from the IP,
    /// so we synthesize a valid, IP-derived hostname (e.g. `192-9-200-136.local.`)
    /// and pass the real IP explicitly — otherwise registration fails and no
    /// receiver can discover us.
    pub fn register_sender(
        &self,
        name: &str,
        ip: &str,
        port: u16,
        sample_rate: u32,
        channels: u16,
    ) -> Result<()> {
        let mut props = HashMap::new();
        props.insert("sample_rate".to_string(), sample_rate.to_string());
        props.insert("channels".to_string(), channels.to_string());
        props.insert("version".to_string(), "1".to_string());

        let host_name = format!("{}.local.", ip.replace(['.', ':'], "-"));

        let service_info = ServiceInfo::new(
            MDNS_SERVICE_TYPE,
            name,
            &host_name,
            ip,
            port,
            props,
        )
        .map_err(|e| {
            crate::error::SyncPlayError::Mdns(format!("Failed to create service info: {e}"))
        })?;

        self.daemon.register(service_info).map_err(|e| {
            crate::error::SyncPlayError::Mdns(format!("Failed to register service: {e}"))
        })?;

        tracing::info!("Registered mDNS service: {name} at {ip}:{port} (host {host_name})");
        Ok(())
    }

    /// Unregister all services.
    pub fn unregister(&self) {
        if let Err(e) = self.daemon.unregister(MDNS_SERVICE_TYPE) {
            tracing::warn!("Failed to unregister mDNS service: {e}");
        }
    }

    /// Shutdown the mDNS daemon.
    #[allow(dead_code)]
    pub fn shutdown(&self) -> Result<()> {
        self.daemon.shutdown().map_err(|e| {
            crate::error::SyncPlayError::Mdns(format!("mDNS shutdown error: {e}"))
        })?;
        Ok(())
    }
}

/// Run the mDNS browser in a background thread.
/// Continuously polls for new/disappeared senders and updates the app state.
pub fn run_discovery_browser(
    daemon: ServiceDaemon,
    app: SharedApp,
    stop: Arc<AtomicBool>,
) {
    let receiver = match daemon.browse(MDNS_SERVICE_TYPE) {
        Ok(rx) => rx,
        Err(e) => {
            tracing::error!("Failed to start mDNS browser: {e}");
            return;
        }
    };

    tracing::info!("mDNS browser started for {MDNS_SERVICE_TYPE}");

    while !stop.load(Ordering::Relaxed) {
        match receiver.recv_timeout(Duration::from_millis(500)) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                let host = info.get_addresses().iter()
                    .next()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| info.get_hostname().to_string());

                let port = info.get_port();
                let sample_rate = info
                    .get_property_val("sample_rate")
                    .and_then(|v| v.and_then(|b| std::str::from_utf8(b).ok()))
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(48000);
                let channels = info
                    .get_property_val("channels")
                    .and_then(|v| v.and_then(|b| std::str::from_utf8(b).ok()))
                    .and_then(|s| s.parse::<u16>().ok())
                    .unwrap_or(2);

                let sender = DiscoveredSender {
                    name: info.get_fullname().to_string(),
                    host,
                    port,
                    sample_rate,
                    channels,
                };

                let mut app = app.lock();
                // Avoid duplicates
                let key = (sender.host.clone(), sender.port);
                let exists = app.receiver.discovered_senders.iter().any(|s| {
                    (s.host.clone(), s.port) == key
                });
                if !exists {
                    tracing::info!("Discovered sender: {} ({}:{})", sender.name, sender.host, sender.port);
                    app.receiver.discovered_senders.push(sender);
                }
            }
            Ok(ServiceEvent::ServiceRemoved(instance_name, _)) => {
                let mut app = app.lock();
                app.receiver.discovered_senders.retain(|s| {
                    !instance_name.contains(&s.name)
                });
                tracing::info!("Sender removed: {instance_name}");
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("mDNS browse error: {e}");
            }
        }
    }

    tracing::info!("mDNS browser stopped");
}
