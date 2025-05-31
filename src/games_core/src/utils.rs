use std::time::Duration;

use anyhow::Error;
use reqwest::{Client, Request, Response};
use thiserror::Error;
use tokio::time::sleep;

pub async fn retry(client: &Client, req: Request) -> Result<Response, Error> {
    let max_attempts = 3;

    let mut attempts = 1;
    let mut backoff = 500;

    loop {
        if attempts > max_attempts {
            break;
        }

        let req = req.try_clone().expect("failed to clone request");

        let resp = match client.execute(req).await {
            Ok(resp) => resp,
            Err(e) => return Err(RetryError::ClientError(e).into()),
        };

        if resp.status().is_success() {
            return Ok(resp);
        } else if resp.status().is_client_error() {
            let e = resp.error_for_status().unwrap_err();
            return Err(RetryError::ClientError(e).into());
        } else if resp.status().is_server_error() {
            attempts += 1;
            backoff *= 2;
            sleep(Duration::from_millis(backoff)).await;
        }
    }
    Err(RetryError::MaxAttempts.into())
}

#[derive(Debug, Error)]
enum RetryError {
    #[error("exceeded max attempts")]
    MaxAttempts,
    #[error("client error: {0}")]
    ClientError(#[from] reqwest::Error),
}
