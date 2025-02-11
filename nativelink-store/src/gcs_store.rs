// gcs_store.rs

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status};
use tonic::transport::{Channel, ClientTlsConfig};
use async_stream::stream;

// Import the generated GCS proto code. (Assume your Bazel build provides this.)
use nativelink_proto::google::storage::v2::{
    storage_client::StorageClient,
    GetObjectRequest, Object,
    WriteObjectRequest, WriteObjectResponse,
    ReadObjectRequest, ReadObjectResponse,
};

use crate::nativelink_error::{Error, Code, make_err};
use crate::nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use crate::nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use crate::nativelink_util::retry::{Retrier, RetryResult};

/// A GCS store that implements the StoreDriver trait using tonic gRPC.
pub struct GCSStore<NowFn> {
    /// The gRPC client for GCS.
    client: StorageClient<Channel>,
    /// The bucket in which to store objects.
    bucket: String,
    /// An optional key prefix that is prepended to every object name.
    key_prefix: String,
    /// A retrier for retryable RPC calls.
    retrier: Retrier,
    /// If nonzero, objects older than “now – consider_expired_after_s” are ignored.
    consider_expired_after_s: i64,
    /// A function that returns the current unix timestamp (in seconds).
    now_fn: NowFn,
}

impl<NowFn> GCSStore<NowFn>
where
    NowFn: Fn() -> i64 + Send + Sync + 'static,
{
    /// Create a new GCSStore.  
    /// The `endpoint` should be the gRPC endpoint (for example, `"http://localhost:50051"`).
    pub async fn new(
        endpoint: String,
        bucket: String,
        key_prefix: Option<String>,
        now_fn: NowFn,
    ) -> Result<Arc<Self>, Error> {
        // In production you might need to set up TLS credentials here.
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| make_err!(Code::Internal, "Invalid endpoint: {e:?}"))?
            .connect()
            .await
            .map_err(|e| make_err!(Code::Unavailable, "Failed to connect to GCS endpoint: {e:?}"))?;

        let client = StorageClient::new(channel);
        // Construct a retrier (for brevity we assume a default configuration).
        let retrier = Retrier::default();

        Ok(Arc::new(Self {
            client,
            bucket,
            key_prefix: key_prefix.unwrap_or_default(),
            retrier,
            consider_expired_after_s: 0, // set as needed via configuration
            now_fn,
        }))
    }

    /// Returns the object name by concatenating the key prefix and the key.
    fn make_object_name(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }

    /// A helper function to perform a “head” operation (using GetObject) and return the object’s size.
    async fn head_object(&mut self, object_name: &str) -> Result<Option<u64>, Error> {
        let request = Request::new(GetObjectRequest {
            bucket: self.bucket.clone(),
            object: object_name.to_string(),
            // Other fields default.
            ..Default::default()
        });
        let response = self.client.get_object(request).await;
        match response {
            Ok(resp) => {
                let obj: Object = resp.into_inner();
                // If the object has a size field, return it.
                Ok(obj.size.map(|s| s as u64))
            }
            Err(status) => {
                if status.code() == tonic::Code::NotFound {
                    Ok(None)
                } else {
                    Err(make_err!(Code::Unavailable, "GCS head_object error: {:?}", status))
                }
            }
        }
    }
}

#[async_trait]
impl<NowFn> StoreDriver for GCSStore<NowFn>
where
    NowFn: Fn() -> i64 + Send + Sync + 'static,
{
    /// Check whether each key exists by querying object metadata.
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        // For simplicity, we create a clone of the client for each call.
        let this = self.get_ref();
        let mut client = this.client.clone();

        for (key, res) in keys.iter().zip(results.iter_mut()) {
            let object_name = this.make_object_name(key);
            let req = Request::new(GetObjectRequest {
                bucket: this.bucket.clone(),
                object: object_name,
                ..Default::default()
            });
            match client.get_object(req).await {
                Ok(response) => {
                    let obj = response.into_inner();
                    *res = obj.size.map(|s| s as u64);
                }
                Err(status) if status.code() == tonic::Code::NotFound => {
                    *res = None;
                }
                Err(status) => {
                    return Err(make_err!(Code::Unavailable, "GCS head_object error: {:?}", status));
                }
            }
        }
        Ok(())
    }

    /// Upload data to GCS by using the WriteObject streaming RPC.
    /// This example sends the object metadata first, then streams chunks of data,
    /// and finally sends a message with finish_write set.
    async fn update(
        self: Pin<&Self>,
        digest: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let this = self.get_ref();
        let object_name = this.make_object_name(&digest);
        let bucket = this.bucket.clone();

        // Create an async stream that yields WriteObjectRequest messages.
        let request_stream = stream! {
            // First message: send the object metadata.
            yield WriteObjectRequest {
                write_offset: 0,
                object_spec: Some(Object {
                    bucket: bucket.clone(),
                    name: object_name.clone(),
                    ..Default::default()
                }),
                // No data in the first message.
                ..Default::default()
            };

            // Now stream data chunks (using a 1 MB buffer in this example).
            let mut buf = vec![0u8; 1024 * 1024];
            let mut offset: i64 = 0;
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break, // EOF reached.
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        yield WriteObjectRequest {
                            write_offset: offset,
                            data,
                            ..Default::default()
                        };
                        offset += n as i64;
                    }
                    Err(e) => {
                        // In a real implementation you might retry here.
                        break;
                    }
                }
            }
            // Final message: indicate that writing is finished.
            yield WriteObjectRequest {
                finish_write: true,
                ..Default::default()
            };
        };

        // Call the WriteObject streaming RPC.
        let request = Request::new(request_stream);
        let mut response_stream = this.client.write_object(request).await
            .map_err(|status| make_err!(Code::Unavailable, "GCS write_object error: {:?}", status))?
            .into_inner();

        // Consume the response stream (ignoring response details for now).
        while let Some(_resp) = response_stream.message().await? {
            // You could inspect the response here if desired.
        }
        Ok(())
    }

    /// Download a portion of an object using the ReadObject streaming RPC.
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let this = self.get_ref();
        let object_name = this.make_object_name(&key);
        let req = Request::new(ReadObjectRequest {
            bucket: this.bucket.clone(),
            object: object_name,
            read_offset: offset as i64,
            // If length is provided, set read_limit; note that a zero read_limit means “no limit”
            read_limit: length.map(|l| l as i64).unwrap_or(0),
            ..Default::default()
        });
        let mut stream = this.client.read_object(req).await
            .map_err(|status| make_err!(Code::Unavailable, "GCS read_object error: {:?}", status))?
            .into_inner();

        while let Some(resp) = stream.message().await? {
            if !resp.data.is_empty() {
                writer.send(Bytes::from(resp.data))
                    .await
                    .map_err(|e| make_err!(Code::Aborted, "Failed to send chunk: {:?}", e))?;
            }
        }
        writer.send_eof()
            .map_err(|e| make_err!(Code::Aborted, "Failed to send EOF: {:?}", e))?;
        Ok(())
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Send + Sync + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync + 'static> {
        self
    }
}
