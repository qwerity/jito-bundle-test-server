use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

#[derive(Clone)]
pub struct SolanaBlockhashFetcher {
    blockhash: Arc<Mutex<String>>,
    last_update: Arc<Mutex<SystemTime>>,
    client: Arc<RpcClient>,
}

impl Default for SolanaBlockhashFetcher {
    fn default() -> Self {
        Self::new("https://api.testnet.solana.com".to_string())
    }
}

impl SolanaBlockhashFetcher {
    pub fn new(solana_url: String) -> Self {
        Self {
            blockhash: Arc::new(Mutex::new(String::new())),
            last_update: Arc::new(Mutex::new(SystemTime::now())),
            client: Arc::new(RpcClient::new_with_commitment(solana_url, CommitmentConfig::confirmed())),
        }
    }
    
    pub fn get_blockhash(&self) -> String {
        self.blockhash.lock().unwrap().clone()
    }
    
    pub async fn update_blockhash(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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