use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use google_cloud_auth::{project::Config, token::DefaultTokenSourceProvider};
use google_cloud_storage::google::storage::v2::{storage_client::StorageClient, Bucket, GetBucketRequest};
use google_cloud_token::TokenSourceProvider as _;
use tonic::{codegen::StdError, Response};
use nativelink_config::stores::GcsSpec;
use nativelink_metric::MetricsComponent;
use nativelink_error::{make_err, make_input_err, Code, Error};
use nativelink_util::{instant_wrapper::InstantWrapper, retry::Retrier};
use rand::{rngs::OsRng, Rng};
use tokio::{sync::Mutex, time::sleep};
use tonic::{service::{interceptor::InterceptedService, Interceptor}, transport::{Channel, ClientTlsConfig}, Request, Status};
use tonic::codec::CompressionEncoding;

// Default max buffer size for retrying upload requests.
// Note: If you change this, adjust the docs in the config.
const DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST: u64 = 5 * 1024 * 1024; // 5MB.

// Default limit for concurrent part uploads per multipart upload.
// Note: If you change this, adjust the docs in the config.
const DEFAULT_MULTIPART_MAX_CONCURRENT_UPLOADS: u64 = 10;

const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com/";
const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/storage";
const DEFAULT_CONNECT_TIMEOUT: u64 = 5;
const DEFAULT_KEEPALIVE_INTERVAL: u64 = 30;
const DEFAULT_TCP_NODELAY: bool = true;
const DEFAULT_HTTP2_ADAPTIVE_WINDOW: bool = true;

/// ChannelConfig groups together all the settings needed to build a gRPC Channel.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    /// The endpoint URL.
    pub endpoint: String,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Whether to enable TCP no-delay.
    pub tcp_nodelay: bool,
    /// Whether to enable HTTP2 adaptive window.
    pub http2_adaptive_window: bool,
    /// HTTP2 keep-alive interval.
    pub http2_keep_alive_interval: Duration,
    /// Optional TLS configuration.
    pub tls_config: Option<ClientTlsConfig>,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.to_string(),
            connect_timeout: Duration::from_secs(DEFAULT_CONNECT_TIMEOUT as u64),
            tcp_nodelay: DEFAULT_TCP_NODELAY,
            http2_adaptive_window: DEFAULT_HTTP2_ADAPTIVE_WINDOW,
            http2_keep_alive_interval: Duration::from_secs(DEFAULT_KEEPALIVE_INTERVAL as u64),
            tls_config: Some(ClientTlsConfig::new().with_native_roots()),
        }
    }
}

impl ChannelConfig {
    /// Builds a gRPC channel using this configuration.
    pub async fn build_channel(&self) -> Result<Channel, Error> {
        let endpoint = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| make_err!(Code::Unavailable, "Failed to create channel: {e}"))?;
        
        // Now we have an Endpoint, so we can configure it.
        let mut builder = endpoint
            .connect_timeout(self.connect_timeout)
            .tcp_nodelay(self.tcp_nodelay)
            .http2_adaptive_window(self.http2_adaptive_window)
            .http2_keep_alive_interval(self.http2_keep_alive_interval);

        if let Some(tls) = &self.tls_config {
            builder = builder.tls_config(tls.clone())?;
        }

        let channel = builder.connect().await?;
        Ok(channel)
    }
}

impl From<&GcsSpec> for ChannelConfig {
    fn from(spec: &GcsSpec) -> Self {
        ChannelConfig {
            endpoint: if spec.endpoint.trim().is_empty() {
                DEFAULT_ENDPOINT.to_string()
            } else {
                spec.endpoint.clone()
            },
            connect_timeout: Duration::from_secs(spec.connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT as u64)),
            tcp_nodelay: spec.tcp_nodely,
            http2_adaptive_window: spec.http2_adaptive_window,
            http2_keep_alive_interval: Duration::from_secs(spec.http2_keep_alive_interval_secs.unwrap_or(DEFAULT_KEEPALIVE_INTERVAL as u64)),
            tls_config: Some(ClientTlsConfig::new().with_native_roots()),
        }
    }
}

pub struct GcsClient {
    inner: Arc<Mutex<StorageClient<InterceptedService<Channel, AuthInterceptor>>>>,
}

impl GcsClient {
    /// Create a new GcsClient from a StorageClient.
    pub fn new(client: StorageClient<InterceptedService<Channel, AuthInterceptor>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(client)),
        }
    }

    /// Example method: get a bucket by forwarding the call to the inner client.
    pub async fn get_bucket(&self, req: Request<GetBucketRequest>) -> Result<Response<Bucket>, Status> {
        let mut client = self.inner.lock().await;
        client.get_bucket(req).await
    }
}

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

#[derive(MetricsComponent)]
pub struct GcsStore<NowFn> {
    storage_client: GcsClient,
    now_fn: NowFn,
    bucket: String,
    retrier: Retrier,
    consider_expired_after_s: i64,
    max_retry_buffer_per_request: u64,
    multipart_max_concurrent_uploads: u64,
}

impl<I, NowFn> GcsStore<NowFn>
where 
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static{
    /// Create a new GcsStore wrapper.
    ///
    /// This performs the authentication, sets up the gRPC channel with an interceptor,
    /// and remembers the target bucket.
    pub async fn new(spec: &GcsSpec, now_fn: NowFn) -> Result<Arc<Self>, Error>  {
        let jitter_amt = spec.retry.jitter;
        let jitter_fn = Arc::new(move |delay: Duration| {
            if jitter_amt == 0. {
                return delay;
            }
            let min = 1. - (jitter_amt / 2.);
            let max = 1. + (jitter_amt / 2.);
            delay.mul_f32(OsRng.gen_range(min..max))
        });
        let channel_config = ChannelConfig::from(spec);
        
        let use_id_token = spec.use_id_token.clone();

        // Convert Vec<String> -> Vec<&str> (only if spec.scopes is Some)
        let scopes_vec: Option<Vec<&str>> = spec.scopes
            .as_ref()
            .map(|vec| vec.iter().map(String::as_str).collect());

            // Convert Option<Vec<&str>> to Option<&[&str]>
            let scopes_slice: Option<&[&str]> = scopes_vec.as_deref();

            let config = Config::default()
            .with_audience(&spec.audience)
            .with_scopes(scopes_slice.unwrap_or(&[])) // ✅ Reference is now valid
            .with_use_id_token(use_id_token);

        let tsp = DefaultTokenSourceProvider::new(config)
            .await
            .map_err(|e| make_input_err!("Failed to create DefaultTokenSourceProvider: {e:?}"))?;
            
            
        let ts = tsp.token_source();
        let token = ts
            .token()
            .await
            .map_err(|e| make_input_err!("Failed to create token: {e:?}"))?;
            

        let auth_interceptor = AuthInterceptor {
            token: token,
        };
        let connect_timeout = Duration::from_secs(spec.connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT));
        let http2_keep_alive_interval =
            Duration::from_secs(spec.http2_keep_alive_interval_secs.unwrap_or(DEFAULT_KEEPALIVE_INTERVAL));

        let channel = channel_config.build_channel().await?;
        
        let storage_client = StorageClient::with_interceptor(channel, auth_interceptor);

        Self::new_with_client_and_jitter(spec, storage_client, jitter_fn, now_fn)
    }

    pub fn new_with_client_and_jitter(
        spec: &GcsSpec,
        storage_client: StorageClient<InterceptedService<Channel, AuthInterceptor>>,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
        now_fn: NowFn,
    ) -> Result<Arc<Self>, Error>  {
        Ok(Arc::new(Self {
            storage_client: GcsClient::new(storage_client),
            bucket: spec.bucket.clone(),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn,
                spec.retry.clone(),
            ),
            now_fn,
            consider_expired_after_s: i64::from(spec.consider_expired_after_s),
            max_retry_buffer_per_request: spec
                .max_retry_buffer_per_request
                .unwrap_or(DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST),
            multipart_max_concurrent_uploads: spec
                .multipart_max_concurrent_uploads
                .map_or(DEFAULT_MULTIPART_MAX_CONCURRENT_UPLOADS, |v| v)
        }))
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
