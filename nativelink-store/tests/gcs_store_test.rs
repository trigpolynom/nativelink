use std::{sync::Arc, time::{SystemTime, Duration}};
use tokio::time::sleep;
use futures::future::{BoxFuture, FutureExt};
use tonic::{Request, Response, Status};
use nativelink_config::stores::{GcsSpec, Retry as StoreRetry};
use nativelink_error::Error;
use nativelink_util::retry::Retrier;
use google_cloud_storage::google::storage::v2::{Bucket, GetBucketRequest};
use nativelink_store::gcs_store::GcsStore;

const BUCKET_NAME: &str = "dummy-bucket-name";
const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const REGION: &str = "testregion";

pub trait GcsClientTrait: Send + Sync {
    fn get_bucket(&self, req: Request<GetBucketRequest>) -> BoxFuture<'static, Result<Response<Bucket>, Status>>;
    // Add other required functions if needed.
}

#[derive(Clone)]
pub struct MockGcsClient {
    pub bucket: Option<Bucket>,
}

impl GcsClientTrait for MockGcsClient {
    fn get_bucket(&self, _req: Request<GetBucketRequest>) -> BoxFuture<'static, Result<Response<Bucket>, Status>> {
        let bucket = self.bucket.clone().unwrap_or_else(|| Bucket {
            etag: "".into(),
            name: "default".into(),
            bucket_id: "default-bucket".into(),
            project: "test-project".into(),
            metageneration: 1,
            location: "US".into(),
            location_type: "region".into(),
            storage_class: "STANDARD".into(),
            rpo: "".into(),
            acl: vec![],
            default_object_acl: vec![],
            lifecycle: None,
            create_time: None,
            cors: vec![],
            update_time: None,
            default_event_based_hold: false,
            labels: Default::default(),
            website: None,
            versioning: None,
            logging: None,
            owner: None,
            encryption: None,
            billing: None,
            retention_policy: None,
            iam_config: None,
            satisfies_pzs: false,
            custom_placement_config: None,
            autoclass: None,
            hierarchical_namespace: None,
            soft_delete_policy: None,
        });
        async move { Ok(Response::new(bucket)) }.boxed()
    }
}

// A wrapper to adapt our mock client to the expected storage_client used within GcsStore.
// Adjust the method signature as needed.
#[derive(Clone)]
struct TestStorageClientWrapper {
    inner: Arc<dyn GcsClientTrait>,
}

impl TestStorageClientWrapper {
    async fn get_bucket(
        &self,
        req: Request<GetBucketRequest>,
    ) -> Result<Response<Bucket>, Status> {
        self.inner.get_bucket(req).await
    }
}

// We assume that GcsStore exposes its fields for testing purposes.
// For this test we construct one with a dummy retrier and now_fn.
#[tokio::test]
async fn test_get_bucket() {
    // Initialize our mock.
    let mock_client = MockGcsClient {
        bucket: None, // Use the default bucket provided by MockGcsClient
    };
    let client_trait: Arc<dyn GcsClientTrait> = Arc::new(mock_client);
    let storage_client = TestStorageClientWrapper { inner: client_trait };

    // Create a dummy retrier that immediately returns (no retries).
    let dummy_retrier = Retrier::new(
        Arc::new(|delay: Duration| Box::pin(sleep(delay))),
        Arc::new(|d: Duration| d),
        StoreRetry {
            max_retries: 0,
            delay: 0.0,
            jitter: 0.0,
            retry_on_errors: None,
        },
    );
    // Manually construct a GcsStore.
    // (Adjust field names and visibility if needed.)
    let store = GcsStore::new_with_client_and_jitter(
        &GcsSpec {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        }, 
        storage_client, 
        Arc::new(move |_delay| Duration::from_secs(0)), 
        MockInstantWrapped::default)?;

    // Call get_bucket and validate the returned bucket.
    let result = store.get_bucket().await;
    assert!(result.is_ok(), "get_bucket returned an error");
    let bucket = result.unwrap();
    assert_eq!(bucket.name, "default");
}
