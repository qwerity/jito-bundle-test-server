use bundle_test_server::{
    utils::{
        hash_utils::{calculate_bundle_hash, calculate_packet_batch_hash}
    },
    proto::{
        auth::{
            auth_service_client::AuthServiceClient,
            GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role,
        },
        block_engine::{
            block_engine_validator_client::BlockEngineValidatorClient,
            SubscribeBundlesRequest, SubscribePacketsRequest,
        },
    }
};
use clap::Parser;
use futures_util::StreamExt;
use solana_keypair::{Keypair, Signer, read_keypair_file};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::anyhow;
use tonic::metadata::MetadataValue;
use tonic::{transport::Channel, Request, Code};
use tracing::{info, warn, error, debug};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind_ip: String,
    #[arg(long, default_value = "21001")]
    bind_port: u16,
    #[arg(long, default_value = "jito_validator.json")]
    keypair_path: String,
    #[arg(long, default_value_t = false)]
    disable_packets_subscription: bool,
    #[arg(long, default_value_t = false)]
    disable_bundles_subscription: bool,
    #[arg(long, default_value = "info")]
    log_level: String,
}

pub struct BundleClient {
    auth_client: AuthServiceClient<Channel>,
    block_engine_client: BlockEngineValidatorClient<Channel>,
    keypair: Arc<Keypair>,
    access_token: Option<String>,
    bundle_counter: Arc<AtomicU64>,
}

impl BundleClient {
    pub async fn new(server_addr: String, keypair_path: String) -> Result<Self, anyhow::Error> {
        let keypair = read_keypair_file(&keypair_path).map_err(|e| anyhow!("{}: {e}", args.keypair_path))?;
        info!("Client using keypair with public key: {}", keypair.pubkey());

        let (auth_client, block_engine_client) = Self::create_clients(&server_addr).await?;

        Ok(Self {
            auth_client,
            block_engine_client,
            keypair: Arc::new(keypair),
            access_token: None,
            bundle_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn create_clients(server_addr: &str) -> Result<(AuthServiceClient<Channel>, BlockEngineValidatorClient<Channel>), anyhow::Error> {
        let uri = format!("http://{}", server_addr);
        let max_retries = 60;

        for attempt in 1..=max_retries {
            info!("Attempting to connect to server {} (attempt {}/{})", server_addr, attempt, max_retries);

            match Channel::from_shared(uri.clone())?.connect().await {
                Ok(channel) => {
                    info!("Successfully connected to server on attempt {}", attempt);
                    let auth_client = AuthServiceClient::new(channel.clone());
                    let block_engine_client = BlockEngineValidatorClient::new(channel);
                    return Ok((auth_client, block_engine_client));
                },
                Err(e) => {
                    warn!("Failed to connect on attempt {}: {}", attempt, e);
                    if attempt < max_retries {
                        info!("Retrying in {} seconds...", 3);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    } else {
                        return Err(anyhow!("Failed to connect after {} attempts: {}", max_retries, e));
                    }
                }
            }
        }

        unreachable!()
    }

    async fn reconnect(&mut self, server_addr: &str) -> Result<(), anyhow::Error> {
        info!("Reconnecting to server...");
        let (auth_client, block_engine_client) = Self::create_clients(server_addr).await?;
        self.auth_client = auth_client;
        self.block_engine_client = block_engine_client;
        Ok(())
    }

    pub async fn authenticate(&mut self) -> Result<(), anyhow::Error> {
        let max_auth_retries = 60;

        for attempt in 1..=max_auth_retries {
            info!("Starting authentication process (attempt {}/{})...", attempt, max_auth_retries);

            match self.try_authenticate().await {
                Ok(()) => {
                    info!("Authentication successful on attempt {}", attempt);
                    return Ok(());
                },
                Err(e) => {
                    warn!("Authentication failed on attempt {}: {}", attempt, e);
                    if attempt < max_auth_retries {
                        info!("Retrying authentication in {} seconds...", 3);
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    } else {
                        return Err(anyhow!("Authentication failed after {} attempts: {}", max_auth_retries, e));
                    }
                }
            }
        }

        unreachable!()
    }

    async fn try_authenticate(&mut self) -> Result<(), anyhow::Error> {
        let challenge_request = GenerateAuthChallengeRequest {
            role: Role::Validator as i32,
            pubkey: self.keypair.pubkey().to_bytes().to_vec(),
        };

        let challenge_response = self.auth_client
            .generate_auth_challenge(Request::new(challenge_request))
            .await?;

        let challenge = challenge_response.into_inner().challenge;
        debug!("Received challenge: {}", challenge);

        let message_to_sign = format!("{}{}", self.keypair.pubkey(), challenge);
        let signature = self.keypair.sign_message(message_to_sign.as_bytes());

        let tokens_request = GenerateAuthTokensRequest {
            challenge,
            client_pubkey: self.keypair.pubkey().to_bytes().to_vec(),
            signed_challenge: signature.as_ref().to_vec(),
        };

        let tokens_response = self.auth_client
            .generate_auth_tokens(Request::new(tokens_request))
            .await?;

        let tokens = tokens_response.into_inner();
        self.access_token = Some(tokens.access_token.unwrap().value);

        Ok(())
    }

    pub async fn subscribe_to_bundles(&mut self, server_addr: String) -> Result<(), anyhow::Error> {
        let max_reconnect_attempts = 60;
        let mut reconnect_attempt = 0;

        loop {
            if self.access_token.is_none() {
                info!("No access token, attempting to authenticate...");
                if let Err(e) = self.authenticate().await {
                    error!("Authentication failed: {}", e);
                    reconnect_attempt += 1;
                    if reconnect_attempt >= max_reconnect_attempts {
                        return Err(anyhow!("Failed to authenticate after {} attempts", max_reconnect_attempts));
                    }
                    info!("Retrying authentication in 3 seconds...");
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            }

            info!("Subscribing to bundles...");

            let mut request = Request::new(SubscribeBundlesRequest {});

            if let Some(token) = &self.access_token {
                let auth_header = MetadataValue::try_from(format!("Bearer {}", token))?;
                request.metadata_mut().insert("authorization", auth_header);
            }

            match self.block_engine_client.subscribe_bundles(request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let bundle_counter = self.bundle_counter.clone();

                    info!("Successfully subscribed to bundles stream");
                    reconnect_attempt = 0;

                    while let Some(bundle_response) = stream.next().await {
                        match bundle_response {
                            Ok(response) => {
                                for bundle_uuid in response.bundles {
                                    if let Some(bundle) = bundle_uuid.bundle {
                                        let count = bundle_counter.fetch_add(1, Ordering::SeqCst) + 1;
                                        let hash = calculate_bundle_hash(&bundle);

                                        info!("|<- Bundle #{}|{}|{}|{} packets|",
                                            count, bundle_uuid.uuid, hash, bundle.packets.len());
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Error receiving bundle: {}", e);
                                if e.code() == Code::Unauthenticated {
                                    warn!("Authentication error, clearing token");
                                    self.access_token = None;
                                }
                                break;
                            }
                        }
                    }

                    warn!("Bundle stream ended, attempting to reconnect...");
                },
                Err(e) => {
                    error!("Failed to subscribe to bundles: {}", e);
                    if e.code() == Code::Unauthenticated {
                        warn!("Authentication error, clearing token");
                        self.access_token = None;
                    }
                }
            }

            reconnect_attempt += 1;
            if reconnect_attempt >= max_reconnect_attempts {
                return Err(anyhow!("Failed to reconnect after {} attempts", max_reconnect_attempts));
            }

            info!("Reconnection attempt {}/{} in 5 seconds...", reconnect_attempt, max_reconnect_attempts);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            if let Err(e) = self.reconnect(&server_addr).await {
                error!("Failed to reconnect: {}", e);
                continue;
            }
        }
    }

    pub async fn subscribe_to_packets(&mut self, server_addr: String) -> Result<(), anyhow::Error> {
        let max_reconnect_attempts = 60;
        let mut reconnect_attempt = 0;
        let mut packet_counter = 0u64;

        loop {
            if self.access_token.is_none() {
                info!("No access token, attempting to authenticate...");
                if let Err(e) = self.authenticate().await {
                    error!("Authentication failed: {}", e);
                    reconnect_attempt += 1;
                    if reconnect_attempt >= max_reconnect_attempts {
                        return Err(anyhow!("Failed to authenticate after {} attempts", max_reconnect_attempts));
                    }
                    info!("Retrying authentication in 3 seconds...");
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    continue;
                }
            }

            info!("Subscribing to packets...");

            let mut request = Request::new(SubscribePacketsRequest {});

            if let Some(token) = &self.access_token {
                let auth_header = MetadataValue::try_from(format!("Bearer {}", token))?;
                request.metadata_mut().insert("authorization", auth_header);
            }

            match self.block_engine_client.subscribe_packets(request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();

                    info!("Successfully subscribed to packets stream");
                    reconnect_attempt = 0;

                    while let Some(packet_response) = stream.next().await {
                        match packet_response {
                            Ok(response) => {
                                if let Some(batch) = response.batch {
                                    let hash = calculate_packet_batch_hash(&batch);
                                    packet_counter += batch.packets.len() as u64;
                                    info!("Received packet batch {} with {} packets (total received: {})",
                                        hash, batch.packets.len(), packet_counter);
                                }
                            },
                            Err(e) => {
                                error!("Error receiving packet: {}", e);
                                if e.code() == Code::Unauthenticated {
                                    warn!("Authentication error, clearing token");
                                    self.access_token = None;
                                }
                                break;
                            }
                        }
                    }

                    warn!("Packet stream ended, attempting to reconnect...");
                },
                Err(e) => {
                    error!("Failed to subscribe to packets: {}", e);
                    if e.code() == Code::Unauthenticated {
                        warn!("Authentication error, clearing token");
                        self.access_token = None;
                    }
                }
            }

            reconnect_attempt += 1;
            if reconnect_attempt >= max_reconnect_attempts {
                return Err(anyhow!("Failed to reconnect after {} attempts", max_reconnect_attempts));
            }

            info!("Reconnection attempt {}/{} in 5 seconds...", reconnect_attempt, max_reconnect_attempts);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            if let Err(e) = self.reconnect(&server_addr).await {
                error!("Failed to reconnect: {}", e);
                continue;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(format!("jito_validator={}", args.log_level))
        .init();


    info!("Bundle Client starting...");
    let server_addr = format!("{}:{}", args.bind_ip, args.bind_port);
    info!("Connecting to server at: {}", server_addr);

    let mut client = BundleClient::new(server_addr.clone(), args.keypair_path.clone()).await?;
    client.authenticate().await?;

    let mut tasks = Vec::new();
    let access_token = client.access_token.clone();

    if !args.disable_bundles_subscription {
        let server_addr_clone = server_addr.clone();
        let keypair_path = args.keypair_path.clone();
        let access_token = access_token.clone();

        tasks.push(tokio::spawn(async move {
            match BundleClient::new(server_addr_clone.clone(), keypair_path).await {
                Ok(mut client_clone) => {
                    client_clone.access_token = access_token;
                    if let Err(e) = client_clone.subscribe_to_bundles(server_addr_clone).await {
                        error!("Bundle subscription error: {}", e);
                    }
                },
                Err(e) => error!("Failed to create bundle client: {}", e),
            }
        }));
    }

    if !args.disable_packets_subscription {
        let server_addr_clone = server_addr.clone();
        let keypair_path = args.keypair_path.clone();
        let access_token = access_token.clone();

        tasks.push(tokio::spawn(async move {
            match BundleClient::new(server_addr_clone.clone(), keypair_path).await {
                Ok(mut client_clone) => {
                    client_clone.access_token = access_token;
                    if let Err(e) = client_clone.subscribe_to_packets(server_addr_clone).await {
                        error!("Packet subscription error: {}", e);
                    }
                },
                Err(e) => error!("Failed to create packet client: {}", e),
            }
        }));
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
