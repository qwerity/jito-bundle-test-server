pub mod solana_blockhash_fetcher;
pub mod auth;
pub mod hash_utils;

pub fn extract_client_addr(request: &tonic::Request<impl std::fmt::Debug>) -> String {
    request.remote_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
