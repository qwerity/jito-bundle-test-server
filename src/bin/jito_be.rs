use bundle_test_server::{
    utils::{
        solana_blockhash_fetcher::SolanaBlockhashFetcher,
        auth::AuthServiceImpl,
        hash_utils::{calculate_bundle_hash, calculate_packet_batch_hash}
    },
    proto::{
        auth::auth_service_server::AuthServiceServer,
        block_engine::{
            BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse,
            GetBlockEngineEndpointRequest, GetBlockEngineEndpointResponse,
            SubscribeBundlesRequest, SubscribeBundlesResponse,
            SubscribePacketsRequest, SubscribePacketsResponse,
            block_engine_validator_server::{BlockEngineValidator, BlockEngineValidatorServer}
        },
        bundle::{Bundle, BundleUuid},
        packet::{Packet, PacketBatch}
    }
};
use clap::Parser;
use futures::stream;
use futures_util::stream::Stream;
use rand::{Rng, rng};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_keypair::{Pubkey, Keypair, Signer, read_keypair_file};
use solana_sdk::instruction::Instruction;
use solana_sdk::transaction::Transaction;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;
use anyhow::anyhow;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, error, debug};
use uuid::Uuid;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    // #[arg(long, default_value = "http://127.0.0.1:8899")]
    #[arg(long, default_value = "https://api.testnet.solana.com")]
    rpc_url: String,
    #[arg(long, default_value = "jito_be.json")]
    keypair_path: String,
    #[arg(long, default_value = "127.0.0.1")]
    bind_ip: String,
    #[arg(long, default_value = "31001")]
    bind_port: u16,
    #[arg(long, default_value = "1000")]
    blockhash_update_interval_ms: u64,
    #[arg(long, default_value = "1000")]
    packets_generation_interval_ms: u64,
    #[arg(long, default_value = "1000")]
    bundle_generation_interval_ms: u64,
    #[arg(long, default_value = "1400000")]
    compute_unit_limit: u32,
    #[arg(long, default_value = "10000")]
    compute_unit_price: u32,
    #[arg(long, default_value_t = false)]
    disable_packets_subscription: bool,
    #[arg(long, default_value_t = false)]
    disable_bundles_subscription: bool,
    #[arg(long, default_value_t = false)]
    skip_blockhash_fetching: bool,
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[derive(Clone)]
pub struct BlockEngineValidatorService {
    blockhash_fetcher: SolanaBlockhashFetcher,
    keypair: Arc<Keypair>,
    args: Args,
    bundle_counter: Arc<AtomicU64>,
}

impl BlockEngineValidatorService {
    pub async fn new(args: Args) -> Result<Self, anyhow::Error> {
        let keypair = read_keypair_file(&args.keypair_path)
            .map_err(|e| anyhow!("{}: {e}", self.config.shiroi_block_engines.keypair_path))?;
        info!("Using keypair with public key: {}", keypair.pubkey());

        let blockhash_fetcher = SolanaBlockhashFetcher::new(
            args.rpc_url.clone(),
            args.skip_blockhash_fetching,
            args.blockhash_update_interval_ms
        ).await?;

        Ok(Self {
            blockhash_fetcher,
            keypair: Arc::new(keypair),
            args,
            bundle_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    fn create_bundle(&self) -> Result<Bundle, anyhow::Error> {
        let blockhash = self.blockhash_fetcher.get_blockhash();
        if blockhash.is_empty() {
            return Err(anyhow!("No blockhash available"));
        }

        let mut rng = rng();
        let tx_count = rng.random_range(1..=5);
        let mut packets = Vec::new();
        let mut amounts = Vec::new();

        for _ in 0..tx_count {
            let amount = rng.random_range(10000..=20000);
            let tx_data = self.create_transfer_transaction(amount, &blockhash)?;
            packets.push(Packet { meta: None, data: tx_data });
            amounts.push(amount);
        }

        debug!("Created bundle with {} real transactions with amounts: {:?}", tx_count, amounts);
        Ok(Bundle {
            header: None,
            packets
        })
    }

    fn create_transfer_transaction(&self, amount_lamports: u64, recent_blockhash: &str) -> Result<Vec<u8>, anyhow::Error> {
        let blockhash = recent_blockhash.parse()?;
        let instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(self.args.compute_unit_limit),
            ComputeBudgetInstruction::set_compute_unit_price(self.args.compute_unit_price.into()),
            Instruction {
                program_id: Pubkey::from(spl_memo::ID.to_bytes()),
                accounts: vec![],
                data: format!("Transfer {} lamports", amount_lamports).into_bytes(),
            },
            solana_system_interface::instruction::transfer(
                &self.keypair.pubkey(),
                &self.keypair.pubkey(),
                amount_lamports
            )
        ];

        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.keypair.pubkey()),
            &[&self.keypair],
            blockhash
        );

        let serialized_tx = bincode::serialize(&transaction)?;
        debug!("Created transaction of {} bytes transferring {} lamports with blockhash: {}",
             serialized_tx.len(), amount_lamports, recent_blockhash);
        Ok(serialized_tx)
    }
}

type PacketResponseStream = Pin<Box<dyn Stream<Item = Result<SubscribePacketsResponse, Status>> + Send>>;
type BundleResponseStream = Pin<Box<dyn Stream<Item = Result<SubscribeBundlesResponse, Status>> + Send>>;

#[tonic::async_trait]
impl BlockEngineValidator for BlockEngineValidatorService {
    type SubscribePacketsStream = PacketResponseStream;

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        if self.args.disable_packets_subscription {
            info!("Packet subscription is disabled");
            return Ok(Response::new(Box::pin(stream::empty())));
        }

        let service = self.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(StdDuration::from_millis(service.args.packets_generation_interval_ms));

            loop {
                interval.tick().await;

                let blockhash = service.blockhash_fetcher.get_blockhash();

                if blockhash.is_empty() {
                    error!("No blockhash available for packet creation");
                    let _ = tx.send(Err(Status::internal("No blockhash available")));
                    continue;
                }

                let mut rng = rng();
                let packet_count = rng.random_range(1..=3);
                let mut packets = Vec::new();
                let mut amounts = Vec::new();

                for _ in 0..packet_count {
                    let amount = rng.random_range(10000..=20000);

                    match service.create_transfer_transaction(amount, &blockhash) {
                        Ok(tx_data) => {
                            let packet = Packet {
                                meta: None,
                                data: tx_data,
                            };
                            packets.push(packet);
                            amounts.push(amount);
                        },
                        Err(e) => {
                            error!("Failed to create packet: {}", e);
                            if tx.send(Err(Status::internal(format!("Failed to create packet: {}", e)))).is_err() {
                                debug!("Packet client disconnected while sending error, stopping task");
                                break;
                            }
                            continue;
                        }
                    }
                }

                if !packets.is_empty() {
                    let batch = PacketBatch { packets };
                    let hash = calculate_packet_batch_hash(&batch);

                    info!("Generated packet batch {} with {} packets (amounts: {:?})", hash, batch.packets.len(), amounts);

                    let response = SubscribePacketsResponse {
                        header: None,
                        batch: Some(batch),
                    };

                    if tx.send(Ok(response)).is_err() {
                        debug!("Packet client disconnected, stopping packet generation task");
                        break;
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    type SubscribeBundlesStream = BundleResponseStream;

    async fn subscribe_bundles(
        &self,
        _request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        if self.args.disable_bundles_subscription {
            info!("Bundles subscription is disabled");
            return Ok(Response::new(Box::pin(stream::empty())));
        }

        let service = self.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(StdDuration::from_millis(service.args.bundle_generation_interval_ms));

            loop {
                interval.tick().await;

                match service.create_bundle() {
                    Ok(bundle) => {
                        let bundle_count = service.bundle_counter.fetch_add(1, Ordering::SeqCst) + 1;
                        let bundle_hash = calculate_bundle_hash(&bundle);
                        let uuid = Uuid::new_v4().to_string();

                        info!("|-> Bundle #{}|{}|{}|{} packets|", bundle_count, uuid, bundle_hash, bundle.packets.len());

                        let response = SubscribeBundlesResponse {
                            bundles: vec![
                                BundleUuid {
                                    uuid,
                                    bundle: Some(bundle),
                                },
                            ]
                        };

                        if tx.send(Ok(response)).is_err() {
                            debug!("Bundle client disconnected, stopping bundle generation task");
                            break;
                        }
                    },
                    Err(e) => {
                        error!("Failed to create bundle: {}", e);
                        if tx.send(Err(Status::internal(format!("Failed to create bundle: {}", e)))).is_err() {
                            debug!("Bundle client disconnected while sending error, stopping task");
                            break;
                        }
                    }
                }
            }
        });

        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        let response = BlockBuilderFeeInfoResponse {
            commission: 5,
            pubkey: self.keypair.pubkey().to_string(),
        };

        Ok(Response::new(response))
    }
    async fn get_block_engine_endpoints(&self, _: Request<GetBlockEngineEndpointRequest>)
        -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(format!("jito_be={}", args.log_level))
        .init();

    let be_server_addr = format!("{}:{}", args.bind_ip, args.bind_port).parse()?;
    info!("Block Engine Validator Server listening on {}", be_server_addr);

    let validator_service = BlockEngineValidatorService::new(args).await?;
    Server::builder()
        .http2_keepalive_interval(Some(StdDuration::from_secs(30)))
        .http2_keepalive_timeout(Some(StdDuration::from_secs(5)))
        .add_service(BlockEngineValidatorServer::new(validator_service))
        .add_service(AuthServiceServer::new(AuthServiceImpl))
        .serve(be_server_addr)
        .await?;
    Ok(())
}
