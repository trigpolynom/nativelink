use std::{borrow::Cow, pin::Pin, sync::Arc, time::{Duration, SystemTime}};

use bytes::Bytes;
use futures::{stream::{self, unfold, FuturesUnordered}, TryStreamExt};
use google_cloud_auth::{project::Config, token::DefaultTokenSourceProvider};
use google_cloud_storage::google::storage::v2::{storage_client::StorageClient, Bucket, GetBucketRequest, GetObjectRequest, Object, UpdateBucketRequest, WriteObjectRequest, WriteObjectResponse};
use google_cloud_token::TokenSourceProvider as _;
use tonic::{async_trait, codegen::StdError, Response, Streaming};
use nativelink_config::stores::{ErrorCode, GcsSpec, Retry};
use nativelink_metric::{MetricsComponent};
use nativelink_error::{make_err, make_input_err, Code, Error};
use nativelink_util::{buf_channel::{DropCloserReadHalf, DropCloserWriteHalf}, health_utils::{HealthStatus, HealthStatusIndicator}, instant_wrapper::InstantWrapper, retry::{Retrier, RetryResult}, store_trait::{StoreDriver, StoreKey, UploadSizeInfo}};
use rand::{rngs::OsRng, Rng};
use tokio::{sync::Mutex, time::sleep};
use tonic::{service::{interceptor::InterceptedService, Interceptor}, transport::{Channel, ClientTlsConfig}, Request, Status};
use tonic::codec::CompressionEncoding;
use google_cloud_auth::project::Config as AuthConfig;
use prost_types::FieldMask;

use crate::cas_utils::is_zero_digest;

// S3 parts cannot be smaller than this number. See:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const MIN_MULTIPART_SIZE: u64 = 5 * 1024 * 1024; // 5MB.

// S3 parts cannot be larger than this number. See:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const MAX_MULTIPART_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5GB.

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

    pub async fn get_bucket(&self, req: Request<GetBucketRequest>) -> Result<Response<Bucket>, Status> {
        let mut client = self.inner.lock().await;
        client.get_bucket(req).await
    }

    pub async fn update_bucket(&self, req: Request<UpdateBucketRequest>) -> Result<Response<Bucket>, Status> {
        let mut client = self.inner.lock().await;
        client.update_bucket(req).await
    }

    pub async fn get_object(&self, req: Request<GetObjectRequest>) -> Result<Response<Object>, Status> {
        let mut client = self.inner.lock().await;
        client.get_object(req).await
    }

    pub async fn put_object(&self, reqs: Vec<WriteObjectRequest>) -> Result<Response<WriteObjectResponse>, Status> {
        let mut client = self.inner.lock().await;
        let req_stream = stream::iter(reqs);
        client.write_object(tonic::Request::new(req_stream)).await
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
    key_prefix: String,
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
            key_prefix: spec.key_prefix.as_ref().unwrap_or(&String::new()).clone(),
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
    pub async fn get_bucket(&self) -> Result<Bucket, tonic::Status> {
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

    fn make_gcs_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str(),)
    }

    pub async fn has(self: Pin<&Self>, digest: &StoreKey<'_>) -> Result<Option<u64>, Error> {
        self.retrier
            .retry(unfold((), move |state| async move {
                // Assume make_gcs_path is analogous to make_s3_path.
                let gcs_path = self.make_gcs_path(&digest.borrow());
                let request = GetObjectRequest {
                    if_metageneration_match: None,
                    if_metageneration_not_match: None,
                    bucket: self.bucket.clone(),
                    object: gcs_path.clone(),
                    generation: 0, // or leave unset if appropriate
                    if_generation_match: None,
                    if_generation_not_match: None,
                    read_mask: Some(prost_types::FieldMask { paths: vec!["size".to_string(), "update_time".to_string()] }),
                    common_object_request_params: None,
                    soft_deleted: None,
                    restore_token: "".to_string(),
                };
                let mut req = Request::new(request);
                req.metadata_mut().insert(
                    "x-goog-request-params",
                    format!("bucket={}&object={}", self.bucket, gcs_path).parse().unwrap(),
                );
                let result = self.storage_client.get_object(req).await;
                match result {
                    Ok(response) => {
                        let object = response.into_inner();
                        if self.consider_expired_after_s != 0 {
                            if let Some(last_modified) = object.update_time {
                                let now_s = (self.now_fn)().unix_timestamp() as i64;
                                if last_modified.seconds + self.consider_expired_after_s <= now_s {
                                    return Some((RetryResult::Ok(None), state));
                                }
                            }
                        }
                        let length = object.size;
                        if length >= 0 {
                            return Some((RetryResult::Ok(Some(length as u64)), state));
                        }
                        Some((
                            RetryResult::Err(make_err!(
                                Code::InvalidArgument,
                                "Negative content length in GCS: {length:?}",
                            )),
                            state,                                
                        ))
                    }
                    Err(gcs_error) => match gcs_error.code() {
                         tonic::Code::NotFound =>
                            Some((RetryResult::Ok(None), state)),
                        other => Some((RetryResult::Retry(make_err!(
                            Code::Unavailable,
                            "Unhandled GetObjectError in GCS: {other:?}"
                        )), state)),
                    },
                }
            }))
            .await
    }
}

#[async_trait]
impl<I, NowFn> HealthStatusIndicator for GcsStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    fn get_name(&self) -> &'static str {
        "GcsStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}


#[async_trait]
impl<I, NowFn> StoreDriver for GcsStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        keys.iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
                // We need to do a special pass to ensure our zero key exist.
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                    return Ok::<_, Error>(());
                }
                *result = self.has(key).await?;
                Ok::<_, Error>(())
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let gcs_path = &self.make_gcs_path(&digest.borrow());

        let max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) | UploadSizeInfo::MaxSize(sz) => sz,
        };

        if max_size < MIN_MULTIPART_SIZE && matches!(upload_size, UploadSizeInfo::ExactSize(_)) {
            let UploadSizeInfo::ExactSize(sz) = upload_size else {
                unreachable!("upload_size must be UploadSizeInfo::ExactSize here");
            };
            reader.set_max_recent_data_size(
                u64::try_from(self.max_retry_buffer_per_request)
                    .err_tip(|| "Could not convert max_retry_buffer_per_request to u64")?,
            );

            return self.
                retrier
                .retry(
                    unfold(
                        reader, gcs_path.clone()),
                        move |(mut reader, gcs_path)| async move {
                            let content = match reader.consume(Some(sz as usize)).await {
                                Ok(content) => content,
                                Err(e) => return Some((RetryResult::Err(e), (reader, gcs_path))),
                            };
                            let conn = match self
                        }
                    )
                )
        }
    }

    
}

