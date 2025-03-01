use std::{borrow::Cow, pin::Pin, sync::Arc, time::{Duration, SystemTime}};

use bytes::Bytes;
use futures::{stream::FuturesUnordered, TryStreamExt};
use google_cloud_auth::{project::Config, token::DefaultTokenSourceProvider};
use google_cloud_storage::google::storage::v2::{storage_client::StorageClient, Bucket, GetBucketRequest, UpdateBucketRequest};
use google_cloud_token::TokenSourceProvider as _;
use tonic::{async_trait, codegen::StdError, Response};
use nativelink_config::stores::{ErrorCode, GcsSpec, Retry};
use nativelink_metric::{MetricsComponent};
use nativelink_error::{make_err, make_input_err, Code, Error};
use nativelink_util::{buf_channel::{DropCloserReadHalf, DropCloserWriteHalf}, health_utils::{HealthStatus, HealthStatusIndicator}, instant_wrapper::InstantWrapper, retry::Retrier, store_trait::{StoreDriver, StoreKey, UploadSizeInfo}};
use rand::{rngs::OsRng, Rng};
use tokio::{sync::Mutex, time::sleep};
use tonic::{service::{interceptor::InterceptedService, Interceptor}, transport::{Channel, ClientTlsConfig}, Request, Status};
use tonic::codec::CompressionEncoding;
use google_cloud_auth::project::Config as AuthConfig;

use crate::cas_utils::is_zero_digest;

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
        let s3_path = &self.make_s3_path(&digest.borrow());

        let max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) | UploadSizeInfo::MaxSize(sz) => sz,
        };

        // Note(allada) It might be more optimal to use a different
        // heuristic here, but for simplicity we use a hard coded value.
        // Anything going down this if-statement will have the advantage of only
        // 1 network request for the upload instead of minimum of 3 required for
        // multipart upload requests.
        //
        // Note(allada) If the upload size is not known, we go down the multipart upload path.
        // This is not very efficient, but it greatly reduces the complexity of the code.
        if max_size < MIN_MULTIPART_SIZE && matches!(upload_size, UploadSizeInfo::ExactSize(_)) {
            let UploadSizeInfo::ExactSize(sz) = upload_size else {
                unreachable!("upload_size must be UploadSizeInfo::ExactSize here");
            };
            reader.set_max_recent_data_size(
                u64::try_from(self.max_retry_buffer_per_request)
                    .err_tip(|| "Could not convert max_retry_buffer_per_request to u64")?,
            );
            return self
                .retrier
                .retry(unfold(reader, move |mut reader| async move {
                    // We need to make a new pair here because the aws sdk does not give us
                    // back the body after we send it in order to retry.
                    let (mut tx, rx) = make_buf_channel_pair();

                    // Upload the data to the S3 backend.
                    let result = {
                        let reader_ref = &mut reader;
                        let (upload_res, bind_res) = tokio::join!(
                            self.s3_client
                                .put_object()
                                .bucket(&self.bucket)
                                .key(s3_path.clone())
                                .content_length(sz as i64)
                                .body(ByteStream::from_body_1_x(BodyWrapper {
                                    reader: rx,
                                    size: sz,
                                }))
                                .send()
                                .map_ok_or_else(|e| Err(make_err!(Code::Aborted, "{e:?}")), |_| Ok(())),
                            // Stream all data from the reader channel to the writer channel.
                            tx.bind_buffered(reader_ref)
                        );
                        upload_res
                            .merge(bind_res)
                            .err_tip(|| "Failed to upload file to s3 in single chunk")
                    };

                    // If we failed to upload the file, check to see if we can retry.
                    let retry_result = result.map_or_else(|mut err| {
                        // Ensure our code is Code::Aborted, so the client can retry if possible.
                        err.code = Code::Aborted;
                        let bytes_received = reader.get_bytes_received();
                        if let Err(try_reset_err) = reader.try_reset_stream() {
                            event!(
                                Level::ERROR,
                                ?bytes_received,
                                err = ?try_reset_err,
                                "Unable to reset stream after failed upload in S3Store::update"
                            );
                            return RetryResult::Err(err
                                .merge(try_reset_err)
                                .append(format!("Failed to retry upload with {bytes_received} bytes received in S3Store::update")));
                        }
                        let err = err.append(format!("Retry on upload happened with {bytes_received} bytes received in S3Store::update"));
                        event!(
                            Level::INFO,
                            ?err,
                            ?bytes_received,
                            "Retryable S3 error"
                        );
                        RetryResult::Retry(err)
                    }, |()| RetryResult::Ok(()));
                    Some((retry_result, reader))
                }))
                .await;
        }

        let upload_id = &self
            .retrier
            .retry(unfold((), move |()| async move {
                let retry_result = self
                    .s3_client
                    .create_multipart_upload()
                    .bucket(&self.bucket)
                    .key(s3_path)
                    .send()
                    .await
                    .map_or_else(
                        |e| {
                            RetryResult::Retry(make_err!(
                                Code::Aborted,
                                "Failed to create multipart upload to s3: {e:?}"
                            ))
                        },
                        |CreateMultipartUploadOutput { upload_id, .. }| {
                            upload_id.map_or_else(
                                || {
                                    RetryResult::Err(make_err!(
                                        Code::Internal,
                                        "Expected upload_id to be set by s3 response"
                                    ))
                                },
                                RetryResult::Ok,
                            )
                        },
                    );
                Some((retry_result, ()))
            }))
            .await?;

        // S3 requires us to upload in parts if the size is greater than 5GB. The part size must be at least
        // 5mb (except last part) and can have up to 10,000 parts.
        let bytes_per_upload_part =
            (max_size / (MIN_MULTIPART_SIZE - 1)).clamp(MIN_MULTIPART_SIZE, MAX_MULTIPART_SIZE);

        let upload_parts = move || async move {
            // This will ensure we only have `multipart_max_concurrent_uploads` * `bytes_per_upload_part`
            // bytes in memory at any given time waiting to be uploaded.
            let (tx, mut rx) = mpsc::channel(self.multipart_max_concurrent_uploads);

            let read_stream_fut = async move {
                let retrier = &Pin::get_ref(self).retrier;
                // Note: Our break condition is when we reach EOF.
                for part_number in 1..i32::MAX {
                    let write_buf = reader
                        .consume(Some(usize::try_from(bytes_per_upload_part).err_tip(|| "Could not convert bytes_per_upload_part to usize")?))
                        .await
                        .err_tip(|| "Failed to read chunk in s3_store")?;
                    if write_buf.is_empty() {
                        break; // Reached EOF.
                    }

                    tx.send(retrier.retry(unfold(
                        write_buf,
                        move |write_buf| {
                            async move {
                                let retry_result = self
                                    .s3_client
                                    .upload_part()
                                    .bucket(&self.bucket)
                                    .key(s3_path)
                                    .upload_id(upload_id)
                                    .body(ByteStream::new(SdkBody::from(write_buf.clone())))
                                    .part_number(part_number)
                                    .send()
                                    .await
                                    .map_or_else(
                                        |e| {
                                            RetryResult::Retry(make_err!(
                                                Code::Aborted,
                                                "Failed to upload part {part_number} in S3 store: {e:?}"
                                            ))
                                        },
                                        |mut response| {
                                            RetryResult::Ok(
                                                CompletedPartBuilder::default()
                                                    // Only set an entity tag if it exists. This saves
                                                    // 13 bytes per part on the final request if it can
                                                    // omit the `<ETAG><ETAG/>` string.
                                                    .set_e_tag(response.e_tag.take())
                                                    .part_number(part_number)
                                                    .build(),
                                            )
                                        },
                                    );
                                Some((retry_result, write_buf))
                            }
                        }
                    ))).await.map_err(|_| make_err!(Code::Internal, "Failed to send part to channel in s3_store"))?;
                }
                Result::<_, Error>::Ok(())
            }.fuse();

            let mut upload_futures = FuturesUnordered::new();

            let mut completed_parts = Vec::with_capacity(
                usize::try_from(cmp::min(
                    MAX_UPLOAD_PARTS as u64,
                    (max_size / bytes_per_upload_part) + 1,
                ))
                .err_tip(|| "Could not convert u64 to usize")?,
            );
            tokio::pin!(read_stream_fut);
            loop {
                if read_stream_fut.is_terminated() && rx.is_empty() && upload_futures.is_empty() {
                    break; // No more data to process.
                }
                tokio::select! {
                    result = &mut read_stream_fut => result?, // Return error or wait for other futures.
                    Some(upload_result) = upload_futures.next() => completed_parts.push(upload_result?),
                    Some(fut) = rx.recv() => upload_futures.push(fut),
                }
            }

            // Even though the spec does not require parts to be sorted by number, we do it just in case
            // there's an S3 implementation that requires it.
            completed_parts.sort_unstable_by_key(|part| part.part_number);

            self.retrier
                .retry(unfold(completed_parts, move |completed_parts| async move {
                    Some((
                        self.s3_client
                            .complete_multipart_upload()
                            .bucket(&self.bucket)
                            .key(s3_path)
                            .multipart_upload(
                                CompletedMultipartUploadBuilder::default()
                                    .set_parts(Some(completed_parts.clone()))
                                    .build(),
                            )
                            .upload_id(upload_id)
                            .send()
                            .await
                            .map_or_else(
                                |e| {
                                    RetryResult::Retry(make_err!(
                                        Code::Aborted,
                                        "Failed to complete multipart upload in S3 store: {e:?}"
                                    ))
                                },
                                |_| RetryResult::Ok(()),
                            ),
                        completed_parts,
                    ))
                }))
                .await
        };
        // Upload our parts and complete the multipart upload.
        // If we fail attempt to abort the multipart upload (cleanup).
        upload_parts()
            .or_else(move |e| async move {
                Result::<(), _>::Err(e).merge(
                    // Note: We don't retry here because this is just a best attempt.
                    self.s3_client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(s3_path)
                        .upload_id(upload_id)
                        .send()
                        .await
                        .map_or_else(
                            |e| {
                                let err = make_err!(
                                    Code::Aborted,
                                    "Failed to abort multipart upload in S3 store : {e:?}"
                                );
                                event!(Level::INFO, ?err, "Multipart upload error");
                                Err(err)
                            },
                            |_| Ok(()),
                        ),
                )
            })
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in filesystem store get_part")?;
            return Ok(());
        }

        let s3_path = &self.make_s3_path(&key);
        let end_read_byte = length
            .map_or(Some(None), |length| Some(offset.checked_add(length)))
            .err_tip(|| "Integer overflow protection triggered")?;

        self.retrier
            .retry(unfold(writer, move |writer| async move {
                let result = self
                    .s3_client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(s3_path)
                    .range(format!(
                        "bytes={}-{}",
                        offset + writer.get_bytes_written(),
                        end_read_byte.map_or_else(String::new, |v| v.to_string())
                    ))
                    .send()
                    .await;

                let mut s3_in_stream = match result {
                    Ok(head_object_output) => head_object_output.body,
                    Err(sdk_error) => match sdk_error.into_service_error() {
                        GetObjectError::NoSuchKey(e) => {
                            return Some((
                                RetryResult::Err(make_err!(
                                    Code::NotFound,
                                    "No such key in S3: {e}"
                                )),
                                writer,
                            ));
                        }
                        other => {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::Unavailable,
                                    "Unhandled GetObjectError in S3: {other:?}",
                                )),
                                writer,
                            ));
                        }
                    },
                };

                // Copy data from s3 input stream to the writer stream.
                while let Some(maybe_bytes) = s3_in_stream.next().await {
                    match maybe_bytes {
                        Ok(bytes) => {
                            if bytes.is_empty() {
                                // Ignore possible EOF. Different implimentations of S3 may or may not
                                // send EOF this way.
                                continue;
                            }
                            if let Err(e) = writer.send(bytes).await {
                                return Some((
                                    RetryResult::Err(make_err!(
                                        Code::Aborted,
                                        "Error sending bytes to consumer in S3: {e}"
                                    )),
                                    writer,
                                ));
                            }
                        }
                        Err(e) => {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::Aborted,
                                    "Bad bytestream element in S3: {e}"
                                )),
                                writer,
                            ));
                        }
                    }
                }
                if let Err(e) = writer.send_eof() {
                    return Some((
                        RetryResult::Err(make_err!(
                            Code::Aborted,
                            "Failed to send EOF to consumer in S3: {e}"
                        )),
                        writer,
                    ));
                }
                Some((RetryResult::Ok(()), writer))
            }))
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gcs_store_initialization() {
        let spec = GcsSpec {
            endpoint: "https://storage.googleapis.com".to_string(),
            bucket: "testnativelink".to_string(),
            audience: "https://storage.googleapis.com".to_string(),
            consider_expired_after_s: 3600,
            retry: Retry {
                max_retries: 5,
                delay: 2.0,
                jitter: 0.1,
                retry_on_errors: Some(vec![ErrorCode::Cancelled]),
            },
            max_retry_buffer_per_request: None,
            multipart_max_concurrent_uploads: None,
            scopes: None,
            use_id_token: false,
            connect_timeout_secs: None,
            http2_keep_alive_interval_secs: None,
            tcp_nodely: true,
            http2_adaptive_window: true,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        };

        let now_fn = || SystemTime::now();
        let store = GcsStore::new(&spec, now_fn).await;
        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn test_gcs_store_bucket_retrieval() {
        // Here you'd mock or set up a test environment for Google Cloud.
        // We'll just check the function can be called without panic.
        let spec = GcsSpec {
            endpoint: "https://storage.googleapis.com".to_string(),
            bucket: "fake-bucket".to_string(),
            audience: "https://storage.googleapis.com".to_string(),
            consider_expired_after_s: 3600,
            retry: Retry {
                max_retries: 5,
                delay: 1.0,
                jitter: 0.0,
                retry_on_errors: None,
            },
            max_retry_buffer_per_request: None,
            multipart_max_concurrent_uploads: None,
            scopes: Some(vec!["https://www.googleapis.com/auth/storage".to_string()]),
            use_id_token: false,
            connect_timeout_secs: None,
            http2_keep_alive_interval_secs: None,
            tcp_nodely: true,
            http2_adaptive_window: true,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        };

        let now_fn = || SystemTime::now();
        if let Ok(store) = GcsStore::new(&spec, now_fn).await {
            // The actual GCS call will fail if the bucket doesn't exist
            // or if credentials are missing, but we ensure no panic.
            let _ = store.get_bucket().await.err();
        } else {
            panic!("Failed to create GcsStore");
        }
    }
}