use std::sync::Arc;
use solana_sdk::signature::{Keypair, Signer, read_keypair_file};

#[derive(Clone)]
pub struct KeypairManager {
    pub keypair: Arc<Keypair>,
    pubkey_str: String,
}

impl KeypairManager {
    pub async fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let keypair = read_keypair_file(path)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) })?;
        let pubkey_str = keypair.pubkey().to_string();
        log::info!("Loaded keypair with public key: {}", pubkey_str);
        Ok(Self {
            keypair: Arc::new(keypair),
            pubkey_str,
        })
    }
    
    pub fn get_public_key(&self) -> String {
        self.pubkey_str.clone()
    }
} 