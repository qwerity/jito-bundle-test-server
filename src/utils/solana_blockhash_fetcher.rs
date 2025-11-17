use std::sync::{Arc, Mutex};
use std::time::{SystemTime, Duration as StdDuration};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use rand::Rng;

#[derive(Clone)]
pub struct SolanaBlockhashFetcher {
    blockhash: Arc<Mutex<String>>,
    last_update: Arc<Mutex<SystemTime>>,
    client: Arc<RpcClient>,
    skip_rpc_fetch: bool,
}

impl Default for SolanaBlockhashFetcher {
    fn default() -> Self {
        // Create a minimal Args-like structure for default
        Self::new_with_params("https://api.testnet.solana.com".to_string(), false, 1000)
    }
}

impl SolanaBlockhashFetcher {
    pub async fn new(
        rpc_url: String,
        skip_blockhash_fetching: bool,
        blockhash_update_interval_ms: u64
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let fetcher = Self::new_with_params(
            rpc_url,
            skip_blockhash_fetching,
            blockhash_update_interval_ms
        );

        if skip_blockhash_fetching {
            log::info!("Blockhash fetching is disabled, using random blockhashes");
            fetcher.update_blockhash().await?;
        } else {
            fetcher.update_blockhash().await?;

            let fetcher_clone = fetcher.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(StdDuration::from_millis(blockhash_update_interval_ms));
                loop {
                    interval.tick().await;
                    if let Err(e) = fetcher_clone.update_blockhash().await {
                        log::warn!("Error updating blockhash: {}", e);
                    }
                }
            });
        }

        Ok(fetcher)
    }

    fn new_with_params(solana_url: String, skip_rpc_fetch: bool, _update_interval_ms: u64) -> Self {
        Self {
            blockhash: Arc::new(Mutex::new(String::new())),
            last_update: Arc::new(Mutex::new(SystemTime::now())),
            client: Arc::new(RpcClient::new_with_commitment(solana_url, CommitmentConfig::confirmed())),
            skip_rpc_fetch,
        }
    }

    pub fn get_blockhash(&self) -> String {
        if self.skip_rpc_fetch {
            Self::generate_random_blockhash()
        } else {
            self.blockhash.lock().unwrap().clone()
        }
    }

    pub fn set_blockhash(&self, blockhash: String) {
        *self.blockhash.lock().unwrap() = blockhash;
        *self.last_update.lock().unwrap() = SystemTime::now();
    }

    pub async fn update_blockhash(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.skip_rpc_fetch {
            let random_blockhash = Self::generate_random_blockhash();
            self.set_blockhash(random_blockhash);
            log::debug!("Generated random blockhash (RPC fetch disabled)");
            Ok(())
        } else {
            match self.client.get_latest_blockhash() {
                Ok(blockhash) => {
                    let blockhash_str = blockhash.to_string();
                    *self.blockhash.lock().unwrap() = blockhash_str.clone();
                    *self.last_update.lock().unwrap() = SystemTime::now();
                    log::debug!("Updated Solana blockhash: {}", blockhash_str);
                    Ok(())
                },
                Err(e) => Err(Box::new(e))
            }
        }
    }

    fn generate_random_blockhash() -> String {
        let mut rng = rand::rng();
        let random_bytes: [u8; 32] = rng.random();
        bs58::encode(random_bytes).into_string()
    }
}
