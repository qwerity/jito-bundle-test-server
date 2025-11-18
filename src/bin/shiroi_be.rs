use bundle_test_server::{
    proto::{
        auth::auth_service_server::AuthServiceServer,
        be_proxy::{
            be_proxy_bundle_service_server::{BeProxyBundleService, BeProxyBundleServiceServer},
            SendBundleRequest, SendBundleResponse, SubscribeBundleResultsRequest
        },
        bundle::{Bundle, BundleUuid},
    },
    utils::{
        auth::{AuthServiceImpl, AuthInterceptor}
    },
};
use anyhow::Result;
use clap::Parser;
use std::hash::{Hash, Hasher, DefaultHasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

// Global counter for received bundles
static BUNDLE_COUNTER: AtomicU64 = AtomicU64::new(0);

// Global broadcast channel for bundle notifications
static BUNDLE_BROADCASTER: OnceLock<broadcast::Sender<BundleUuid>> = OnceLock::new();

#[derive(Debug, Default)]
pub struct BeProxyServiceImpl;

impl BeProxyServiceImpl {
    /// Error message constants
    const ERROR_NO_BUNDLE_DATA: &'static str = "BundleUuid contains no bundle data";
    const ERROR_BUNDLE_UUID_REQUIRED: &'static str = "bundle_uuid is required";

    /// Extract client address string from request for logging
    fn extract_client_addr(request: &Request<impl std::fmt::Debug>) -> String {
        request.remote_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Get or initialize the global bundle broadcaster
    fn get_bundle_broadcaster() -> &'static broadcast::Sender<BundleUuid> {
        BUNDLE_BROADCASTER.get_or_init(|| {
            let (tx, _) = broadcast::channel(1000); // Buffer up to 1000 bundle notifications
            tx
        })
    }

    /// Calculate a deterministic hash for the bundle for verification purposes
    fn calculate_bundle_hash(bundle: &Bundle) -> String {
        let mut hasher = DefaultHasher::new();

        // Hash the number of packets
        bundle.packets.len().hash(&mut hasher);

        // Hash each packet's data
        for (index, packet) in bundle.packets.iter().enumerate() {
            index.hash(&mut hasher);
            packet.data.hash(&mut hasher);

            // Hash packet metadata if available
            if let Some(meta) = &packet.meta {
                meta.size.hash(&mut hasher);
                meta.addr.hash(&mut hasher);
                meta.port.hash(&mut hasher);
            }
        }

        // Hash bundle header timestamp if available
        if let Some(header) = &bundle.header {
            if let Some(ts) = &header.ts {
                ts.seconds.hash(&mut hasher);
                ts.nanos.hash(&mut hasher);
            }
        }

        // Return hash as hex string
        format!("{:016x}", hasher.finish())
    }

    /// Remove one transaction (packet) from the bundle and return the modified bundle
    fn remove_one_transaction(bundle_uuid: BundleUuid, addr_str: &str) -> BundleUuid {
        let mut modified_bundle_uuid = bundle_uuid;
        if let Some(ref mut modified_bundle) = modified_bundle_uuid.bundle {
            if !modified_bundle.packets.is_empty() {
                // Remove the first packet (transaction)
                modified_bundle.packets.remove(0);
                debug!("🔧 [{}] Removed 1 transaction. Bundle now has {} packets",
                      addr_str, modified_bundle.packets.len());
            }
        }
        modified_bundle_uuid
    }

    /// Broadcast bundle to all subscribers and log the results
    fn broadcast_bundle_to_subscribers(bundle_uuid: &BundleUuid, addr_str: &str) {
        let broadcaster = Self::get_bundle_broadcaster();

        // Calculate hash of the bundle for logging
        let bundle_hash = if let Some(ref bundle) = bundle_uuid.bundle {
            Self::calculate_bundle_hash(bundle)
        } else {
            "no_bundle".to_string()
        };

        match broadcaster.send(bundle_uuid.clone()) {
            Ok(subscriber_count) => {
                if subscriber_count > 0 {
                    info!("📡 [{}] Broadcasting bundle UUID: {}, hash: {} to {} subscribers",
                          addr_str, bundle_uuid.uuid, bundle_hash, subscriber_count);
                }
            }
            Err(_) => {
                // No subscribers currently listening, which is fine
                debug!("📡 [{}] No active subscribers for bundle broadcast (UUID: {}, hash: {})",
                       addr_str, bundle_uuid.uuid, bundle_hash);
            }
        }
    }
}

#[tonic::async_trait]
impl BeProxyBundleService for BeProxyServiceImpl {
    type SubscribeBundleResultsStream = ReceiverStream<Result<BundleUuid, Status>>;

    async fn subscribe_bundle_results(
        &self,
        request: Request<SubscribeBundleResultsRequest>,
    ) -> Result<Response<Self::SubscribeBundleResultsStream>, Status> {
        let addr_str = Self::extract_client_addr(&request);

        info!("📡 [{}] Client subscribing to bundle results", addr_str);

        // Get the global broadcaster and subscribe to it
        let broadcaster = Self::get_bundle_broadcaster();
        let mut broadcast_rx = broadcaster.subscribe();

        // Create a new channel for this specific subscriber
        let (tx, rx) = mpsc::channel(100);

        // Clone address string for the spawned task
        let addr_str_clone = addr_str.clone();

        // Spawn a task to forward broadcast messages to this subscriber
        tokio::spawn(async move {
            loop {
                match broadcast_rx.recv().await {
                    Ok(bundle_uuid) => {
                        if tx.send(Ok(bundle_uuid)).await.is_err() {
                            // Subscriber disconnected - get remaining count
                            let remaining_subscribers = Self::get_bundle_broadcaster().receiver_count().saturating_sub(1);
                            info!("📡 [{}] Subscriber disconnected (remaining subscribers: {})",
                                  addr_str_clone, remaining_subscribers);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Broadcaster closed
                        let remaining_subscribers = Self::get_bundle_broadcaster().receiver_count().saturating_sub(1);
                        warn!("📡 [{}] Bundle broadcaster closed (remaining subscribers: {})",
                              addr_str_clone, remaining_subscribers);
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // Subscriber lagged behind
                        warn!("📡 [{}] Subscriber lagged, skipped {} messages (continuing...)",
                              addr_str_clone, skipped);
                        // Continue receiving
                    }
                }
            }
        });

        // Get current subscriber count
        let subscriber_count = broadcaster.receiver_count();
        info!("✅ [{}] Bundle results subscription established (total subscribers: {})",
              addr_str, subscriber_count);

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn send_bundle(
        &self,
        request: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        let addr_str = Self::extract_client_addr(&request);
        let req = request.into_inner();

        // The be_proxy SendBundleRequest contains a BundleUuid (which includes the bundle)
        if let Some(bundle_uuid) = req.bundle_uuid {
            // Increment the global bundle counter
            let bundle_number = BUNDLE_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;

            if let Some(bundle) = &bundle_uuid.bundle {
                // Calculate bundle hash using custom hash function
                let bundle_hash = Self::calculate_bundle_hash(bundle);

                info!("📦 |Bundle <- #{}|{}|{}|{} packets|",
                      bundle_number,
                      bundle_uuid.uuid,
                      bundle_hash,
                      bundle.packets.len());

                // Log packet details for debugging
                for (i, packet) in bundle.packets.iter().enumerate() {
                    debug!("   📝 [{}] Packet {}: {} bytes", addr_str, i + 1, packet.data.len());
                    if let Some(meta) = &packet.meta {
                        debug!("       Meta: {}:{}, size: {}", meta.addr, meta.port, meta.size);
                    }
                }

                // Log bundle header info if available
                if let Some(header) = &bundle.header {
                    if let Some(ts) = &header.ts {
                        debug!("   ⏰ [{}] Bundle timestamp: {}.{:09}", addr_str, ts.seconds, ts.nanos);
                    }
                }

                // Remove one transaction from the bundle
                let modified_bundle_uuid = Self::remove_one_transaction(bundle_uuid, &addr_str);

                // Log the modified bundle info
                if let Some(ref modified_bundle) = modified_bundle_uuid.bundle {
                    let modified_hash = Self::calculate_bundle_hash(modified_bundle);
                    info!("🔧 |Bundle -> #{}|{}|{}|{} packets|",
                          bundle_number,
                          modified_bundle_uuid.uuid,
                          modified_hash,
                          modified_bundle.packets.len());
                }

                // Broadcast the modified bundle to all subscribers
                Self::broadcast_bundle_to_subscribers(&modified_bundle_uuid, &addr_str);

                // Return success response with modified bundle UUID
                Ok(Response::new(SendBundleResponse {
                    success: true,
                    error_message: "".to_string(),
                    bundle_uuid: Some(modified_bundle_uuid),
                }))
            } else {
                warn!("❌ [{}] BundleUuid contains no bundle data", addr_str);
                Ok(Response::new(SendBundleResponse {
                    success: false,
                    error_message: Self::ERROR_NO_BUNDLE_DATA.to_string(),
                    bundle_uuid: Some(bundle_uuid),
                }))
            }
        } else {
            warn!("❌ [{}] Empty bundle_uuid received", addr_str);
            Ok(Response::new(SendBundleResponse {
                success: false,
                error_message: Self::ERROR_BUNDLE_UUID_REQUIRED.to_string(),
                bundle_uuid: None,
            }))
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "bundle-block-engine")]
#[command(about = "A Jito block engine server that receives and prints bundles and packets")]
struct Args {
    /// Bind IP address for GRPC server
    #[arg(long, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    bind_ip: IpAddr,

    /// Bind port address for GRPC server
    #[arg(long, default_value_t = 41001)]
    bind_port: u16,

    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(format!("shiroi_be={}", args.log_level))
        .init();

    info!("🚀 Starting Jito Block Engine Server with BeProxyBundleService");

    let addr = SocketAddr::new(args.bind_ip, args.bind_port);
    info!("📍 Binding to: {}", addr);

    info!("✅ Services initialized, starting server...");
    info!("🔧 Available services:");
    info!("   - AuthService (authentication and token management)");
    info!("   - BeProxyBundleService (bundle proxy operations)");
    info!("🔒 Auth interceptor enabled for protected endpoints");
    info!("🎯 Waiting for client connections...");

    match Server::builder()
        .add_service(AuthServiceServer::new(AuthServiceImpl))
        .add_service(
            BeProxyBundleServiceServer::with_interceptor(
                BeProxyServiceImpl,
                AuthInterceptor,
            )
        )
        .serve(addr)
        .await
    {
        Ok(_) => {
            info!("✅ Server shutdown gracefully");
        }
        Err(e) => {
            error!("❌ Server error: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
