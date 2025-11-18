use crate::proto::auth::{
    auth_service_server::AuthService,
    GenerateAuthChallengeRequest, GenerateAuthChallengeResponse,
    GenerateAuthTokensRequest, GenerateAuthTokensResponse,
    RefreshAccessTokenRequest, RefreshAccessTokenResponse,
    Token,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status, service::Interceptor};
use tracing::{error, info, warn, debug};
use crate::utils::extract_client_addr;

#[derive(Clone)]
pub struct AuthInterceptor;

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let metadata = request.metadata();

        // For simplicity, we'll check the authorization header for all requests
        // In a real implementation, you might want to check the method name
        // but tonic doesn't expose it easily in the interceptor
        match metadata.get("authorization") {
            Some(token) => {
                let token_str = token.to_str()
                    .map_err(|_| Status::unauthenticated("Invalid token format"))?;

                // Simple validation - in production, verify JWT or lookup in database
                if token_str.starts_with("Bearer access_token_") || token_str.starts_with("Bearer refreshed_access_token_") {
                    debug!("✅ Valid token provided");
                    Ok(request)
                } else {
                    warn!("❌ Invalid token format");
                    Err(Status::unauthenticated("Invalid token"))
                }
            }
            None => {
                warn!("❌ No authorization header provided");
                Err(Status::unauthenticated("Missing authorization header"))
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct AuthServiceImpl;

#[tonic::async_trait]
impl AuthService for AuthServiceImpl {
    async fn generate_auth_challenge(
        &self,
        request: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        let addr_str = extract_client_addr(&request);
        let req = request.into_inner();

        let pubkey_str = bs58::encode(&req.pubkey).into_string();
        info!("🔐 [{}] Auth Challenge Request - Role: {:?}, Pubkey: {}", addr_str, req.role, pubkey_str);

        let challenge = format!("challenge_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());

        info!("✅ [{}] Auth challenge generated successfully for pubkey: {}", addr_str, pubkey_str);

        Ok(Response::new(GenerateAuthChallengeResponse { challenge }))
    }

    async fn generate_auth_tokens(
        &self,
        request: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        let addr_str = extract_client_addr(&request);
        let req = request.into_inner();

        let pubkey_str = bs58::encode(&req.client_pubkey).into_string();
        info!("🎫 [{}] Generate Tokens Request - Challenge: {}, Pubkey: {}",
              addr_str, req.challenge, pubkey_str);

        // Simulate signature verification
        if req.signed_challenge.is_empty() {
            error!("❌ [{}] Authentication failed: Empty signature for pubkey: {}", addr_str, pubkey_str);
            return Err(Status::unauthenticated("Invalid signature"));
        }

        let now = SystemTime::now();
        let access_token = Token {
            value: format!("access_token_{}", now.duration_since(UNIX_EPOCH).unwrap().as_secs()),
            expires_at_utc: Some(prost_types::Timestamp::from(now + std::time::Duration::from_secs(3600))),
        };
        let refresh_token = Token {
            value: format!("refresh_token_{}", now.duration_since(UNIX_EPOCH).unwrap().as_secs()),
            expires_at_utc: Some(prost_types::Timestamp::from(now + std::time::Duration::from_secs(86400))),
        };

        info!("✅ [{}] Authentication successful! Tokens generated for pubkey: {}", addr_str, pubkey_str);

        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(access_token),
            refresh_token: Some(refresh_token),
        }))
    }

    async fn refresh_access_token(
        &self,
        request: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        let req = request.into_inner();
        info!("🔄 Refresh Token Request - Token: {}", req.refresh_token);

        let access_token = Token {
            value: format!("refreshed_access_token_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()),
            expires_at_utc: Some(prost_types::Timestamp::from(SystemTime::now() + std::time::Duration::from_secs(3600))),
        };

        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(access_token),
        }))
    }
}
