// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::unfold;
use futures::TryStreamExt;
use google_cloud_storage::google::storage::v2::storage_client::StorageClient;
use nativelink_config::stores::GcsSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf,
};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use rand::rngs::OsRng;
use rand::Rng;
use tokio::time::sleep;
use tracing::{error, info};

use crate::cas_utils::is_zero_digest;

// A placeholder for your retry configuration.
#[derive(Clone)]
pub struct RetrySpec {
    pub jitter: f32,
    // ... other retry parameters
}

// We use the generated GCS client from the tonic build:
use storage_client::StorageClient;

// Our GCSStore struct, similar to the S3Store:
#[derive(MetricsComponent)]
pub struct GCSStore<NowFn, T> {
    // The gRPC client for Google Cloud Storage.
    gcs_client: Arc<StorageClient<T>>,
    now_fn: NowFn,
    #[metric(help = "The bucket name for the GCS store")]
    bucket: String,
    #[metric(help = "The key prefix for the GCS store")]
    key_prefix: String,
    retrier: Retrier,
    #[metric(help = "The number of seconds to consider an object expired")]
    consider_expired_after_s: i64,
    #[metric(help = "The number of bytes to buffer for retrying requests")]
    max_retry_buffer_per_request: usize,
}

impl<NowFn, T, I> GCSStore<NowFn, T>
where
    // T is the underlying transport (typically a tonic Channel or similar).
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Send + Sync + 'static,
    T::Error: Into<StdError> + Send + Sync,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
    I: InstantWrapper,
{
    /// Creates a new GCSStore from a gRPC channel and specification.
    pub async fn new(spec: &GcsSpec, channel: T, now_fn: NowFn) -> Result<Arc<Self>, Box<dyn StdError>> {
        // Create the gRPC client using the provided channel.
        let client = StorageClient::new(channel);

        // Build a jitter function for retrying requests.
        let jitter_amt = spec.retry.jitter;
        let jitter_fn = Arc::new(move |delay: Duration| {
            if jitter_amt == 0. {
                return delay;
            }
            let min = 1.0 - (jitter_amt / 2.0);
            let max = 1.0 + (jitter_amt / 2.0);
            delay.mul_f32(OsRng.gen_range(min..max))
        });

        Self::new_with_client_and_jitter(spec, client, jitter_fn, now_fn)
    }

    /// Creates a new GCSStore using an already-configured client and jitter function.
    pub fn new_with_client_and_jitter(
        spec: &GcsSpec,
        client: StorageClient<T>,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
        now_fn: NowFn,
    ) -> Result<Arc<Self>, Box<dyn StdError>> {
        Ok(Arc::new(Self {
            gcs_client: Arc::new(client),
            now_fn,
            bucket: spec.bucket.clone(),
            key_prefix: spec.key_prefix.clone().unwrap_or_default(),
            retrier: Retrier::new(
                // The retrier needs an async sleep function.
                Arc::new(|duration| Box::pin(sleep(duration))),
                jitter_fn,
                spec.retry.clone(),
            ),
            consider_expired_after_s: spec.consider_expired_after_s,
            max_retry_buffer_per_request: spec
                .max_retry_buffer_per_request
                .unwrap_or(DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST),
        }))
    }

    /// Constructs the full object path by combining the key prefix and the provided key.
    fn make_gcs_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }

    /// An example method to upload an object.
    ///
    /// In a real implementation, you would call the appropriate method on
    /// `self.gcs_client` using the gRPC API. Here, we wrap the call in our retry logic.
    pub async fn upload_object(&self, key: &StoreKey<'_>, data: Vec<u8>) -> Result<(), Box<dyn StdError>> {
        let object_path = self.make_gcs_path(key);
        // Wrap the upload call in the retrier for robust error handling.
        self.retrier.run(|| async {
            // Here you would create and send the gRPC request.
            // For example, if the gRPC method were called `upload_object`:
            //
            // let request = tonic::Request::new(UploadObjectRequest {
            //     bucket: format!("projects/_/buckets/{}", self.bucket),
            //     object: object_path.clone(),
            //     data: data.clone(),
            // });
            //
            // self.gcs_client.upload_object(request).await?;
            //
            // For this example, we’ll just return Ok(()); replace this with real logic.
            Ok(())
        }).await
    }