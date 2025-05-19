use crate::proto::auth::{self, Token};
use crate::proto::auth::auth_service_server::AuthService;
use chrono::{Duration, Utc};
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct Auth;

#[tonic::async_trait]
impl AuthService for Auth {
    async fn generate_auth_challenge(
        &self,
        _request: Request<auth::GenerateAuthChallengeRequest>,
    ) -> Result<Response<auth::GenerateAuthChallengeResponse>, Status> {
        Ok(Response::new(auth::GenerateAuthChallengeResponse {
            challenge: "012345678".to_string(),
        }))
    }

    async fn generate_auth_tokens(
        &self,
        _request: Request<auth::GenerateAuthTokensRequest>,
    ) -> Result<Response<auth::GenerateAuthTokensResponse>, Status> {
        Ok(Response::new(auth::GenerateAuthTokensResponse {
            access_token: Some(Token {
                value: "".to_string(),
                expires_at_utc: Some(Timestamp {
                    seconds: (Utc::now() + Duration::seconds(60)).timestamp(),
                    nanos: 0,
                }),
            }),
            refresh_token: Some(Token {
                value: "".to_string(),
                expires_at_utc: Some(Timestamp {
                    seconds: (Utc::now() + Duration::seconds(60)).timestamp(),
                    nanos: 0,
                }),
            }),
        }))
    }

    async fn refresh_access_token(
        &self,
        _request: Request<auth::RefreshAccessTokenRequest>,
    ) -> Result<Response<auth::RefreshAccessTokenResponse>, Status> {
        Ok(Response::new(auth::RefreshAccessTokenResponse {
            access_token: Some(Token {
                value: "012345678".to_string(),
                expires_at_utc: Some(Timestamp {
                    seconds: (Utc::now() + Duration::seconds(60)).timestamp(),
                    nanos: 0,
                }),
            }),
        }))
    }
} 