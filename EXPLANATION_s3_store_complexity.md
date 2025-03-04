
# Understanding Key Parts of s3_store.rs

This file explains the most complex parts of the S3 store implementation in plain language.

---

## 1. TLS Connector and Connection Retry

- **What it does:**  
  The `TlsConnector` sets up an HTTPS connection to S3. It wraps the connection logic with retry behavior. In other words, if a connection attempt fails, it retries using a combination of delay and random jitter (variation) to avoid overwhelming the server.

- **Key points:**  
  - It builds a connection that can work with HTTP/2 if available.  
  - It uses a `Retrier`, a helper component that repeatedly calls the connection until it succeeds or a maximum retry count is reached.  
  - The retry uses an `unfold` stream—a Rust async way of repeatedly running a task—to attempt connection and then return a valid stream.

---

## 2. The Single-Chunk Upload Path

- **What it does:**  
  When a file is small (less than 5MB), it is uploaded in one go. This is simpler and uses only one network request.

- **How it works:**  
  - It creates a stream pair (writer and reader) so that data can be buffered and retried if needed.  
  - The data is sent using a `put_object()` call.
  - If the upload fails, the code marks it with a retryable error, resets the stream (so data can be sent again), and retries the operation.

- **Why retries matter:**  
  Network glitches might interrupt the upload. The retry mechanism helps the upload succeed without the caller needing to know about these interruptions.

---

## 3. Multipart Upload for Large Files

- **What it does:**  
  For large files, S3 requires a multipart upload where the file is split into smaller parts that are uploaded concurrently.

- **How it works:**
  - **Create an upload session:**  
    A multipart upload session is initiated by calling `create_multipart_upload()`. This returns an ID that groups all parts together.
  
  - **Splitting into parts:**  
    The file is read in chunks whose size is calculated between S3’s minimum (5MB) and maximum allowed sizes.  
    A channel limits the number of concurrent uploads so that memory usage stays within bounds.

  - **Uploading parts:**  
    Each part is uploaded using `upload_part()`.  
    Each upload happens inside a retry loop so that if one part fails, that part is retried without aborting the whole upload.

  - **Finalizing the upload:**  
    Once all parts are uploaded, they are sorted by part number (important for S3). Then the call to `complete_multipart_upload()` finalizes the process.

  - **Error Cleanup:**  
    If something goes wrong and the multipart upload cannot complete, a call to `abort_multipart_upload()` is made to clean up the session on S3.

- **Why is it complex?**  
  You are coordinating multiple asynchronous tasks concurrently, handling errors and retrying each task individually while ensuring that the final request uses all the parts in the correct order.

---

## 4. Retry Mechanism Using `Retrier` and `unfold`

- **What it does:**  
  Instead of writing manual loops for retries, the code uses a helper called `Retrier` combined with a function called `unfold` to generate an asynchronous stream of retry attempts.

- **How it works:**  
  - The `unfold` function is used to model the retry operation as a series of attempts.  
  - Each attempt returns either a successful result or instructs to retry (with a delay provided by the retrier).

- **Benefits:**  
  This design avoids deeply nested loops and clearly separates the retry logic from the core upload logic.

---

## Summary

For someone new to Rust, these are the takeaways:
- The code uses async programming heavily (with `async`/`await`) to manage network calls.
- It relies on retries to handle failures gracefully.
- Concurrency (using channels and futures) is used to manage large file uploads by breaking them into parts.
- Each part of the code is designed to be resilient to network failures through careful error handling and retry logic.

This layered, modular approach makes the S3 store both powerful and robust even when facing unpredictable network conditions.
