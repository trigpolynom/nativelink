// gcs_store_test.rs

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Stream;
    use nativelink_proto::google::storage::v2::{
        storage_server::{Storage, StorageServer},
        GetObjectRequest, Object,
        WriteObjectRequest, WriteObjectResponse,
        ReadObjectRequest, ReadObjectResponse,
    };
    use tonic::{Request, Response, Status};
    use async_trait::async_trait;
    use async_stream::stream;
    use std::net::SocketAddr;
    use tokio::sync::Mutex;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A fake implementation of the Storage service for testing.
    #[derive(Default)]
    struct FakeStorage {}

    #[async_trait]
    impl Storage for FakeStorage {
        async fn get_object(
            &self,
            request: Request<GetObjectRequest>,
        ) -> Result<Response<Object>, Status> {
            let req = request.into_inner();
            // For testing, if the object name is "exists", return a dummy object with size 1234.
            if req.object == "exists" {
                Ok(Response::new(Object {
                    bucket: req.bucket,
                    name: req.object,
                    size: Some(1234),
                    ..Default::default()
                }))
            } else {
                Err(Status::not_found("Object not found"))
            }
        }

        async fn write_object(
            &self,
            request: Request<tonic::Streaming<WriteObjectRequest>>,
        ) -> Result<Response<tonic::codec::Streaming<WriteObjectResponse>>, Status> {
            let mut stream = request.into_inner();
            // Consume the stream; in a real implementation you would write data.
            while let Some(_msg) = stream.message().await? { }
            let output = stream! {
                yield WriteObjectResponse {
                    persisted_size: 42,
                    ..Default::default()
                };
            };
            Ok(Response::new(Box::pin(output) as _))
        }

        async fn read_object(
            &self,
            _request: Request<ReadObjectRequest>,
        ) -> Result<Response<tonic::codec::Streaming<ReadObjectResponse>>, Status> {
            // Return a stream with one chunk of dummy data.
            let output = stream! {
                yield ReadObjectResponse {
                    data: b"test data".to_vec(),
                    ..Default::default()
                };
            };
            Ok(Response::new(Box::pin(output) as _))
        }
    }

    // Helper to spawn the fake server.
    async fn spawn_fake_server() -> SocketAddr {
        let fake_storage = FakeStorage::default();
        let svc = StorageServer::new(fake_storage);
        let addr: SocketAddr = "[::1]:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn test_head_object_exists() {
        let addr = spawn_fake_server().await;
        let endpoint = format!("http://{}", addr);
        // For testing, our now_fn is a dummy.
        let now_fn = || 0i64;
        let store = GCSStore::new(endpoint, "test-bucket".to_string(), None, now_fn)
            .await
            .unwrap();

        // Use head_object directly.
        let mut store_mut = (*store).clone();
        let size = store_mut.head_object("exists").await.unwrap();
        assert_eq!(size, Some(1234));
    }

    #[tokio::test]
    async fn test_update_and_get_part() {
        let addr = spawn_fake_server().await;
        let endpoint = format!("http://{}", addr);
        let now_fn = || 0i64;
        let store = GCSStore::new(endpoint, "test-bucket".to_string(), None, now_fn)
            .await
            .unwrap();

        // For update(), we create an in–memory reader.
        let data = b"dummy data for upload";
        let (mut reader, _) = crate::nativelink_util::buf_channel::make_test_read_writer(data.to_vec());

        // Call update().
        store.update("dummy-key".into(), reader, UploadSizeInfo::ExactSize(data.len() as u64))
            .await
            .unwrap();

        // For get_part(), create a writer that writes into a Vec.
        let mut output = Vec::new();
        let mut writer = crate::nativelink_util::buf_channel::TestWriteHalf::new(&mut output);
        store.get_part("dummy-key".into(), &mut writer, 0, Some(100))
            .await
            .unwrap();

        // Our fake ReadObject returns "test data".
        assert_eq!(output, b"test data");
    }
}
