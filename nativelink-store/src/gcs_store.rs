use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use google_cloud_auth::{project::Config, token::DefaultTokenSourceProvider};
use google_cloud_storage::google::storage::v2::{storage_client::StorageClient, Bucket, GetBucketRequest};
use google_cloud_token::TokenSourceProvider as _;
use tonic::codegen::StdError;
use nativelink_config::stores::GcsSpec;
use nativelink_metric::MetricsComponent;
use nativelink_error::{make_err, Code, Error};
use nativelink_util::{instant_wrapper::InstantWrapper, retry::Retrier};
use rand::{rngs::OsRng, Rng};
use tokio::time::sleep;
use tonic::{service::{interceptor::InterceptedService, Interceptor}, transport::{Channel, ClientTlsConfig}, Request, Status};
use tonic::codec::CompressionEncoding;

// Default max buffer size for retrying upload requests.
// Note: If you change this, adjust the docs in the config.
const DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST: usize = 5 * 1024 * 1024; // 5MB.

// Default limit for concurrent part uploads per multipart upload.
// Note: If you change this, adjust the docs in the config.
const DEFAULT_MULTIPART_MAX_CONCURRENT_UPLOADS: usize = 10;

const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com/";
const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/storage";
const DEFAULT_CONNECT_TIMEOUT: usize = 5;
const DEFAULT_KEEPALIVE_INTERVAL: usize = 30;
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

fn parse_compression_encoding(encoding: &str) -> Option<CompressionEncoding> {
    match encoding.to_lowercase().as_str() {
        "gzip" => {
            #[cfg(feature = "gzip")]
            {
                Some(CompressionEncoding::Gzip)
            }
            #[cfg(not(feature = "gzip"))]
            {
                None
            }
        },
        "zstd" => {
            #[cfg(feature = "zstd")]
            {
                Some(CompressionEncoding::Zstd)
            }
            #[cfg(not(feature = "zstd"))]
            {
                None
            }
        },
        _ => None,
    }
}

// /// Extension trait to chain optional configuration for the storage client.
// pub trait StorageClientExt: Sized {
//     fn maybe_send_compressed(self, encoding: Option<&str>) -> Self;
//     fn maybe_accept_compressed(self, encoding: Option<&str>) -> Self;
//     fn maybe_max_decoding_message_size(self, size: Option<usize>) -> Self;
//     fn maybe_max_encoding_message_size(self, size: Option<usize>) -> Self;

//     // These methods are provided by the underlying client.
//     fn send_compressed(self, encoding: CompressionEncoding) -> Self;
//     fn accept_compressed(self, encoding: CompressionEncoding) -> Self;
//     fn max_decoding_message_size(self, limit: usize) -> Self;
//     fn max_encoding_message_size(self, limit: usize) -> Self;
// }


// impl<T> StorageClientExt for StorageClient<T>
// where
//     T: tonic::client::GrpcService<tonic::body::BoxBody>,
//     T::Error: Into<StdError>,
//     T::ResponseBody: http_body::Body<Data = Bytes> + Send + 'static,
//     <T::ResponseBody as http_body::Body>::Error: Into<StdError> + Send,
// {
//     fn maybe_send_compressed(self, encoding: Option<&str>) -> Self {
//         if let Some(enc_str) = encoding {
//             if let Some(enc) = parse_compression_encoding(enc_str) {
//                 return self.send_compressed(enc);
//             }
//         }
//         self
//     }

//     fn maybe_accept_compressed(self, encoding: Option<&str>) -> Self {
//         if let Some(enc_str) = encoding {
//             if let Some(enc) = parse_compression_encoding(enc_str) {
//                 return self.accept_compressed(enc);
//             }
//         }
//         self
//     }

//     fn maybe_max_decoding_message_size(self, size: Option<usize>) -> Self {
//         if let Some(limit) = size {
//             return self.max_decoding_message_size(limit);
//         }
//         self
//     }

//     fn maybe_max_encoding_message_size(self, size: Option<usize>) -> Self {
//         if let Some(limit) = size {
//             return self.max_encoding_message_size(limit);
//         }
//         self
//     }

//     // The following simply call the inherent methods.
//     fn send_compressed(self, encoding: CompressionEncoding) -> Self {
//         StorageClient::send_compressed(self, encoding)
//     }
//     fn accept_compressed(self, encoding: CompressionEncoding) -> Self {
//         StorageClient::accept_compressed(self, encoding)
//     }
//     fn max_decoding_message_size(self, limit: usize) -> Self {
//         StorageClient::max_decoding_message_size(self, limit)
//     }
//     fn max_encoding_message_size(self, limit: usize) -> Self {
//         StorageClient::max_encoding_message_size(self, limit)
//     }
// }

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
    storage_client: Arc<StorageClient<InterceptedService<Channel, AuthInterceptor>>>,
    now_fn: NowFn,
    bucket: String,
    retrier: Retrier,
    consider_expired_after_s: i64,
    max_retry_buffer_per_request: usize,
    multipart_max_concurrent_uploads: usize,
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
        
        let use_id_token = spec.use_id_token.unwrap_or(false).clone();

        let config = Config::default()
            .with_audience(&spec.audience)
            .with_scopes(&spec.scopes)
            .with_use_id_token(use_id_token);
        
        let tsp = DefaultTokenSourceProvider::new(config)
            .await?;
        let ts = tsp.token_source();
        let token = ts
            .token()
            .await?;

        let auth_interceptor = AuthInterceptor {
            token: token,
        };
        let connect_timeout = Duration::from_secs(spec.connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT));
        let http2_keep_alive_interval =
            Duration::from_secs(spec.http2_keep_alive_interval_secs.unwrap_or(DEFAULT_KEEPALIVE_INTERVAL));

        // --- Build the gRPC channel ---
        // Adjust the endpoint if necessary. Here we assume the default storage endpoint.
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
            storage_client: Arc::new(storage_client),
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

    // /// Retrieve bucket metadata from GCS.
    // pub async fn get_bucket(&mut self) -> Result<Bucket, tonic::Status> {
    //     let req = GetBucketRequest {
    //         name: self.bucket.clone(),
    //         if_metageneration_match: None,
    //         if_metageneration_not_match: None,
    //         read_mask: None
    //     };

    //     let mut req = Request::new(req);
    //     req.metadata_mut().insert(
    //         "x-goog-request-params",
    //         format!("name={}", self.bucket).parse().expect("valid header"),
    //     );
        
    //     let response = self.storage_client.get_bucket(req).await?;
    //     Ok(response.into_inner())
    // }
}

// #[tokio::main]
// async fn main() -> Result<()> {
//     let bucket_name = "testnativelink";
//     let mut gcs_store = GcsStore::new(bucket_name)
//         .await
//         .context("Failed to create GcsStore")?;

//     match gcs_store.get_bucket().await {
//         Ok(bucket) => println!("Retrieved bucket: {:?}", bucket),
//         Err(status) => eprintln!("Error retrieving bucket: {:?}", status),
//     }

//     Ok(())
// }