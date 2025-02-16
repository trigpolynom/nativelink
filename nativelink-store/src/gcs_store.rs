use std::{any, time::Duration};

use anyhow::{Context, Result};
use google_cloud_auth::{project::Config, token::DefaultTokenSourceProvider};
use google_cloud_storage::google::storage::v2::{storage_client::StorageClient, Bucket, GetBucketRequest};
use google_cloud_token::TokenSourceProvider as _;
use tonic::{service::{interceptor::InterceptedService, Interceptor}, transport::{Channel, ClientTlsConfig}, Request, Status};


#[derive(Clone)]
struct AuthInterceptor {
    token: String,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        let bearer = &self.token;
        req.metadata_mut()
            .insert("authorization", bearer.parse().unwrap());
        Ok(req)
    }
}

#[derive(Debug, Clone)]
pub struct GcsStore {
    storage_client: StorageClient<InterceptedService<Channel, AuthInterceptor>>,
    bucket: String,
}

impl GcsStore {
    /// Create a new GcsStore wrapper.
    ///
    /// This performs the authentication, sets up the gRPC channel with an interceptor,
    /// and remembers the target bucket.
    pub async fn new(bucket: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();

        // --- Authenticate with Google Cloud ---
        let audience = "https://storage.googleapis.com/";
        let scopes = ["https://www.googleapis.com/auth/storage"];
        let config = Config::default()
            .with_audience(audience)
            .with_scopes(&scopes)
            .with_use_id_token(false);
        
        let tsp = DefaultTokenSourceProvider::new(config)
            .await
            .context("Failed to create token source provider")?;
        let ts = tsp.token_source();
        let token = ts
            .token()
            .await
            .map_err(|e| anyhow::Error::msg(e.to_string()))
            .context("Failed to obtain token")?;

        // Create the interceptor using the retrieved token.
        let auth_interceptor = AuthInterceptor {
            token: token,
        };
        let endpoint = "https://storage.googleapis.com".to_string();
        // --- Build the gRPC channel ---
        // Adjust the endpoint if necessary. Here we assume the default storage endpoint.
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| anyhow::Error::msg(e.to_string()))?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| anyhow::Error::msg(e.to_string()))?
            .connect()
            .await
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
        
        let storage_client = StorageClient::with_interceptor(channel, auth_interceptor);

        Ok(Self {
            storage_client,
            bucket,
        })
    }

    /// Retrieve bucket metadata from GCS.
    pub async fn get_bucket(&mut self) -> Result<Bucket, tonic::Status> {
        let req = GetBucketRequest {
            name: self.bucket.clone(),
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            read_mask: None
        };

        let mut req = Request::new(req);
        req.metadata_mut().insert(
            "x-goog-request-params",
            format!("name={}", self.bucket).parse().expect("valid header"),
        );
        
        let response = self.storage_client.get_bucket(req).await?;
        Ok(response.into_inner())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let bucket_name = "testnativelink";
    let mut gcs_store = GcsStore::new(bucket_name)
        .await
        .context("Failed to create GcsStore")?;

    match gcs_store.get_bucket().await {
        Ok(bucket) => println!("Retrieved bucket: {:?}", bucket),
        Err(status) => eprintln!("Error retrieving bucket: {:?}", status),
    }

    Ok(())
}