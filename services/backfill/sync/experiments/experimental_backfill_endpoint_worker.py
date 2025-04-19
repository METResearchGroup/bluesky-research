"""Old deprecated experimental versions of backfill endpoint worker."""
# class PDSEndpointWorkerSynchronous:
#     """Synchronous version of PDSEndpointWorker."""

#     def __init__(self, did_to_pds_endpoint_map: dict[str, str]):
#         self.did_to_pds_endpoint_map = did_to_pds_endpoint_map

#         self.did_pds_tuples: list[tuple[str, str]] = [
#             (did, pds_endpoint)
#             for did, pds_endpoint
#             in did_to_pds_endpoint_map.items()
#         ]

#         # Counters for metrics
#         self.total_requests = 0
#         self.total_successes = 0

#         self.max_retries = 3  # number of times to retry a failed DID before adding to deadletter queue.

#         # token bucket, for managing rate limits.
#         self.token_reset_minutes = 5  # 5 minutes, as per the rate limit spec.
#         self.token_reset_seconds = self.token_reset_minutes * 60
#         self.max_tokens = (
#             MANUAL_RATE_LIMIT  # set conservatively to avoid max rate limit.
#         )
#         self.token_bucket = TokenBucket(
#             max_tokens=self.max_tokens, window_seconds=self.token_reset_seconds
#         )

#         # permanent storage queues.
#         self.write_queues = get_single_write_queues()
#         self.output_results_queue: Queue = self.write_queues["output_results_queue"]
#         self.output_deadletter_queue: Queue = self.write_queues[
#             "output_deadletter_queue"
#         ]

#         # temporary queues.
#         self.temp_results_queue = queue.Queue()
#         self.temp_deadletter_queue = queue.Queue()
#         self.temp_work_queue = queue.Queue()

#         # TODO: change this and add `filter_previously_processed_dids`
#         # to eventually avoid duplication.
#         max_records = 1000
#         # max_records = 100000
#         original_len_did_pds_tuples = len(self.did_pds_tuples)
#         self.did_pds_tuples = filter_previous_did_endpoint_tuples(
#             did_endpoint_tuples=self.did_pds_tuples,
#             results_db=self.output_results_queue,
#             deadletter_db=self.output_deadletter_queue,
#         )
#         filtered_len_did_pds_tuples = len(self.did_pds_tuples)
#         diff_len_did_pds_tuples = original_len_did_pds_tuples - filtered_len_did_pds_tuples
#         logger.info(
#             f"Filtered {diff_len_did_pds_tuples} DIDs from {original_len_did_pds_tuples} original DIDs."
#         )
#         self.did_pds_tuples = self.did_pds_tuples[:max_records]
#         logger.info(
#             f"Truncated DIDs from {original_len_did_pds_tuples} original DIDs to {len(self.did_pds_tuples)} DIDs with cap of {max_records}."
#         )

#         self.total_did_pds_tuples = len(self.did_pds_tuples)

#         # self.dids = filter_previously_processed_dids(
#         #     pds_endpoint=self.pds_endpoint,
#         #     dids=self.dids,
#         #     results_db=self.output_results_queue,
#         #     deadletter_db=self.output_deadletter_queue,
#         # )

#         # add backoff factor to avoid overloading the PDS endpoint
#         # with burst requests.
#         self.backoff_factor = 1.0  # 1 second backoff.

#         # Track a rolling window of response times
#         self.response_times = collections.deque(maxlen=20)

#     def _process_did(self, did: str, pds_endpoint: str):
#         retry_count = 0
#         max_retries = self.max_retries
#         self.total_requests += 1
#         start = time.perf_counter()
#         while retry_count < max_retries:
#             try:
#                 content = None
#                 pdm.BACKFILL_REQUESTS.labels(endpoint=pds_endpoint).inc()
#                 pdm.BACKFILL_INFLIGHT.labels(endpoint=pds_endpoint).inc()
#                 self.token_bucket._acquire()
#                 pdm.BACKFILL_TOKENS_LEFT.labels(endpoint=pds_endpoint).set(
#                     self.token_bucket._tokens
#                 )
#                 response = send_request_to_pds(did=did, pds_endpoint=pds_endpoint)
#                 content = None
#                 if response.status_code == 200:
#                     content: bytes = response.content
#                     self.temp_results_queue.put({"did": did, "content": content})
#                 elif response.status_code == 429:
#                     # Rate limited, put back in queue
#                     time.sleep(1 * (2**retry_count))  # Exponential backoff
#                     self.temp_work_queue.put(did)
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=pds_endpoint, queue_type="work"
#                     ).set(self.temp_work_queue.qsize())
#                     return
#                 else:
#                     # Error case
#                     self.temp_deadletter_queue.put({"did": did, "content": ""})
#                     pdm.BACKFILL_DID_STATUS.labels(
#                         endpoint=pds_endpoint, status="http_error"
#                     ).inc()
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=pds_endpoint, queue_type="deadletter"
#                     ).set(self.temp_deadletter_queue.qsize())
#                     return

#                 # if content:
#                 #     # offload CPU portion to thread pool.
#                 #     # keeping outside of semaphore to avoid blocking the semaphore
#                 #     # with the extra CPU operations.
#                 #     processing_start = time.perf_counter()
#                 #     records = get_records_from_pds_bytes(content)
#                 #     records = [
#                 #         record
#                 #         for record in records
#                 #         if validate_is_valid_bsky_type(record)
#                 #     ]
#                 #     processing_time = time.perf_counter() - processing_start
#                 #     pdm.BACKFILL_PROCESSING_SECONDS.labels(
#                 #         endpoint=pds_endpoint, operation_type="parse_records"
#                 #     ).observe(processing_time)
#                 #     self.temp_results_queue.put({"did": did, "content": records})

#                 #     # Update DID status and success metrics
#                 #     pdm.BACKFILL_DID_STATUS.labels(
#                 #         endpoint=pds_endpoint, status="success"
#                 #     ).inc()
#                 #     self.total_successes += 1

#                 #     # Update success ratio
#                 #     if self.total_requests > 0:
#                 #         success_ratio = self.total_successes / self.total_requests
#                 #         pdm.BACKFILL_SUCCESS_RATIO.labels(
#                 #             endpoint=pds_endpoint
#                 #         ).set(success_ratio)

#                 #     # Update queue size metrics
#                 #     current_results_queue_size = self.temp_results_queue.qsize()
#                 #     pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=pds_endpoint).set(
#                 #         current_results_queue_size
#                 #     )
#                 #     pdm.BACKFILL_QUEUE_SIZES.labels(
#                 #         endpoint=pds_endpoint, queue_type="results"
#                 #     ).set(current_results_queue_size)

#                 #     # After successful fast responses:
#                 #     if processing_time < 1.0:
#                 #         self.backoff_factor = max(
#                 #             self.backoff_factor * 0.8, 1.0
#                 #         )  # Decrease backoff

#                 #     # Track a rolling window of response times
#                 #     self.response_times.append(processing_time)
#                 #     avg_response_time = sum(self.response_times) / len(
#                 #         self.response_times
#                 #     )

#                 #     # Dynamically adjust concurrency based on average response time
#                 #     if avg_response_time > 3.0:  # If responses are getting slow
#                 #         # Reduce effective concurrency temporarily
#                 #         time.sleep(avg_response_time / 2)

#                 #     return

#             except (requests.RequestException, requests.Timeout) as e:
#                 # Track network errors specifically
#                 error_type = (
#                     "timeout" if isinstance(e, requests.Timeout) else "connection"
#                 )
#                 pdm.BACKFILL_NETWORK_ERRORS.labels(
#                     endpoint=pds_endpoint, error_type=error_type
#                 ).inc()

#                 retry_count += 1
#                 # Increment retry counter
#                 pdm.BACKFILL_RETRIES.labels(endpoint=pds_endpoint).inc()

#                 if retry_count < max_retries:
#                     # log and retry with backoff
#                     logger.warning(
#                         f"(PDS endpoint: {pds_endpoint}): Connection error for DID {did}, retry {retry_count}: {e}"
#                     )
#                     time.sleep(1 * (2**retry_count))  # Exponential backoff
#                 else:
#                     # max retries already reached, move to deadletter.
#                     logger.error(
#                         f"(PDS endpoint: {pds_endpoint}): Failed processing DID {did} after {max_retries} retries: {e}"
#                     )
#                     self.temp_deadletter_queue.put({"did": did, "content": ""})
#                     pdm.BACKFILL_DID_STATUS.labels(
#                         endpoint=pds_endpoint, status="network_error"
#                     ).inc()
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=pds_endpoint, queue_type="deadletter"
#                     ).set(self.temp_deadletter_queue.qsize())
#             except Exception as e:
#                 pdm.BACKFILL_ERRORS.labels(endpoint=pds_endpoint).inc()
#                 # Other unexpected errors
#                 logger.error(
#                     f"(PDS endpoint: {pds_endpoint}): Unexpected error processing DID {did}: {e}"
#                 )
#                 self.temp_deadletter_queue.put({"did": did, "content": ""})
#                 pdm.BACKFILL_DID_STATUS.labels(
#                     endpoint=pds_endpoint, status="unexpected_error"
#                 ).inc()
#                 pdm.BACKFILL_QUEUE_SIZES.labels(
#                     endpoint=pds_endpoint, queue_type="deadletter"
#                 ).set(self.temp_deadletter_queue.qsize())
#             finally:
#                 pdm.BACKFILL_INFLIGHT.labels(endpoint=pds_endpoint).dec()

#                 # Track both the summary and histogram for latency
#                 request_latency = time.perf_counter() - start
#                 pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=pds_endpoint).observe(
#                     request_latency
#                 )
#                 pdm.BACKFILL_LATENCY_HISTOGRAM.labels(
#                     endpoint=pds_endpoint
#                 ).observe(request_latency)

#     def flush_to_db(self):
#         results = []
#         deadletters = []
#         current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
#         while not self.temp_results_queue.empty():
#             # results.append(self.temp_results_queue.get())
#             temp_result = self.temp_results_queue.get()
#             records = get_records_from_pds_bytes(temp_result["content"])
#             records = [
#                 record
#                 for record in records
#                 if validate_is_valid_bsky_type(record)
#             ]
#             results.extend(records)
#         while not self.temp_deadletter_queue.empty():
#             deadletters.append(self.temp_deadletter_queue.get())
#         logger.info(
#             f"Flushing {len(results)} results and {len(deadletters)} deadletters to DB..."
#         )
#         if len(results) > 0:
#             self.output_results_queue.batch_add_items_to_queue(
#                 items=results, metadata={"timestamp": current_timestamp}
#             )
#         if len(deadletters) > 0:
#             self.output_deadletter_queue.batch_add_items_to_queue(
#                 items=deadletters, metadata={"timestamp": current_timestamp}
#             )

#     def run(self):
#         for did, pds_endpoint in self.did_pds_tuples:
#             self.temp_work_queue.put((did, pds_endpoint))
#         logger.info(f"Processing {len(self.did_pds_tuples)} DIDs...")
#         current_idx = 0
#         # log_batch_interval = 50
#         log_batch_interval = 1
#         total_dids = len(self.did_pds_tuples)

#         total_threads = 8 # TODO: prob increase?
#         latest_batch = []

#         total_dids_processed = 0
#         total_batches = self.total_did_pds_tuples // total_threads

#         while not self.temp_work_queue.empty():
#             latest_batch = []
#             # Get a batch of DIDs to process
#             for _ in range(total_threads):
#                 if not self.temp_work_queue.empty():
#                     latest_batch.append(self.temp_work_queue.get())
#                     total_dids_processed += 1
#                 else:
#                     break

#             # if current_idx % log_batch_interval == 0:
#             #     logger.info(f"Processing DID {current_idx}/{total_dids}...")

#             if current_idx % total_batches == 0:
#                 logger.info(
#                     f"Processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
#                 )

#             # Process DIDs in parallel using ThreadPoolExecutor
#             with concurrent.futures.ThreadPoolExecutor(
#                 max_workers=total_threads
#             ) as executor:
#                 futures = [
#                     executor.submit(self._process_did, did, pds_endpoint)
#                     for did, pds_endpoint in latest_batch
#                 ]
#                 # Wait for all threads to complete
#                 concurrent.futures.wait(futures)

#             if (
#                 total_dids_processed > 0
#                 and total_dids_processed % default_write_batch_size == 0
#             ):
#                 logger.info(
#                     f"Total DIDs processed so far: {total_dids_processed}. Flushing to DB."
#                 )
#                 self.flush_to_db()
#                 logger.info(
#                     f"Total DIDs processed so far: {total_dids_processed}. Flushed to DB."
#                 )

#             # if current_idx % log_batch_interval == 0:
#             #     logger.info(f"Completed processing {current_idx}/{total_dids} DIDs...")
#             if current_idx % total_batches == 0:
#                 logger.info(
#                     f"Completed processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
#                 )
#             current_idx += 1

#         logger.info(f"Finished processing all DIDs.")


# class PDSEndpointWorkerSynchronous:
#     """Synchronous version of PDSEndpointWorker."""

#     def __init__(self, pds_endpoint: str, dids: list[str]):
#         self.pds_endpoint = pds_endpoint
#         self.dids = dids

#         # Counters for metrics
#         self.total_requests = 0
#         self.total_successes = 0

#         self.max_retries = 3  # number of times to retry a failed DID before adding to deadletter queue.

#         # token bucket, for managing rate limits.
#         self.token_reset_minutes = 5  # 5 minutes, as per the rate limit spec.
#         self.token_reset_seconds = self.token_reset_minutes * 60
#         self.max_tokens = (
#             MANUAL_RATE_LIMIT  # set conservatively to avoid max rate limit.
#         )
#         self.token_bucket = TokenBucket(
#             max_tokens=self.max_tokens, window_seconds=self.token_reset_seconds
#         )

#         # permanent storage queues.
#         self.write_queues = get_write_queues(pds_endpoint=pds_endpoint)
#         self.output_results_queue: Queue = self.write_queues["output_results_queue"]
#         self.output_deadletter_queue: Queue = self.write_queues[
#             "output_deadletter_queue"
#         ]

#         # temporary queues.
#         self.temp_results_queue = queue.Queue()
#         self.temp_deadletter_queue = queue.Queue()
#         self.temp_work_queue = queue.Queue()

#         self.dids = filter_previously_processed_dids(
#             pds_endpoint=self.pds_endpoint,
#             dids=self.dids,
#             results_db=self.output_results_queue,
#             deadletter_db=self.output_deadletter_queue,
#         )

#         # add backoff factor to avoid overloading the PDS endpoint
#         # with burst requests.
#         self.backoff_factor = 1.0  # 1 second backoff.

#         # Track a rolling window of response times
#         self.response_times = collections.deque(maxlen=20)

#     def _process_did(self, did: str):
#         retry_count = 0
#         max_retries = self.max_retries
#         self.total_requests += 1
#         start = time.perf_counter()
#         while retry_count < max_retries:
#             try:
#                 content = None
#                 pdm.BACKFILL_REQUESTS.labels(endpoint=self.pds_endpoint).inc()
#                 pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).inc()
#                 self.token_bucket._acquire()
#                 pdm.BACKFILL_TOKENS_LEFT.labels(endpoint=self.pds_endpoint).set(
#                     self.token_bucket._tokens
#                 )
#                 request_start = time.perf_counter()
#                 response = send_request_to_pds(did=did, pds_endpoint=self.pds_endpoint)
#                 request_latency = time.perf_counter() - request_start
#                 logger.info(f"Request latency: {request_latency} seconds")
#                 pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
#                     request_latency
#                 )
#                 content = None
#                 if response.status_code == 200:
#                     content: bytes = response.content
#                     self.temp_results_queue.put({"did": did, "content": content})
#                 elif response.status_code == 429:
#                     # Rate limited, put back in queue
#                     time.sleep(1 * (2**retry_count))  # Exponential backoff
#                     self.temp_work_queue.put(did)
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=self.pds_endpoint, queue_type="work"
#                     ).set(self.temp_work_queue.qsize())
#                     return
#                 else:
#                     # Error case
#                     self.temp_deadletter_queue.put({"did": did, "content": ""})
#                     pdm.BACKFILL_DID_STATUS.labels(
#                         endpoint=self.pds_endpoint, status="http_error"
#                     ).inc()
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=self.pds_endpoint, queue_type="deadletter"
#                     ).set(self.temp_deadletter_queue.qsize())
#                     return

#                 # if content:
#                 #     # offload CPU portion to thread pool.
#                 #     # keeping outside of semaphore to avoid blocking the semaphore
#                 #     # with the extra CPU operations.
#                 #     processing_start = time.perf_counter()
#                 #     records = get_records_from_pds_bytes(content)
#                 #     records = [
#                 #         record
#                 #         for record in records
#                 #         if validate_is_valid_bsky_type(record)
#                 #     ]
#                 #     processing_time = time.perf_counter() - processing_start
#                 #     pdm.BACKFILL_PROCESSING_SECONDS.labels(
#                 #         endpoint=pds_endpoint, operation_type="parse_records"
#                 #     ).observe(processing_time)
#                 #     self.temp_results_queue.put({"did": did, "content": records})

#                 #     # Update DID status and success metrics
#                 #     pdm.BACKFILL_DID_STATUS.labels(
#                 #         endpoint=pds_endpoint, status="success"
#                 #     ).inc()
#                 #     self.total_successes += 1

#                 #     # Update success ratio
#                 #     if self.total_requests > 0:
#                 #         success_ratio = self.total_successes / self.total_requests
#                 #         pdm.BACKFILL_SUCCESS_RATIO.labels(
#                 #             endpoint=pds_endpoint
#                 #         ).set(success_ratio)

#                 #     # Update queue size metrics
#                 #     current_results_queue_size = self.temp_results_queue.qsize()
#                 #     pdm.BACKFILL_QUEUE_SIZE.labels(endpoint=pds_endpoint).set(
#                 #         current_results_queue_size
#                 #     )
#                 #     pdm.BACKFILL_QUEUE_SIZES.labels(
#                 #         endpoint=pds_endpoint, queue_type="results"
#                 #     ).set(current_results_queue_size)

#                 #     # After successful fast responses:
#                 #     if processing_time < 1.0:
#                 #         self.backoff_factor = max(
#                 #             self.backoff_factor * 0.8, 1.0
#                 #         )  # Decrease backoff

#                 #     # Track a rolling window of response times
#                 #     self.response_times.append(processing_time)
#                 #     avg_response_time = sum(self.response_times) / len(
#                 #         self.response_times
#                 #     )

#                 #     # Dynamically adjust concurrency based on average response time
#                 #     if avg_response_time > 3.0:  # If responses are getting slow
#                 #         # Reduce effective concurrency temporarily
#                 #         time.sleep(avg_response_time / 2)

#                 #     return

#             except (requests.RequestException, requests.Timeout) as e:
#                 # Track network errors specifically
#                 error_type = (
#                     "timeout" if isinstance(e, requests.Timeout) else "connection"
#                 )
#                 pdm.BACKFILL_NETWORK_ERRORS.labels(
#                     endpoint=self.pds_endpoint, error_type=error_type
#                 ).inc()

#                 retry_count += 1
#                 # Increment retry counter
#                 pdm.BACKFILL_RETRIES.labels(endpoint=self.pds_endpoint).inc()

#                 if retry_count < max_retries:
#                     # log and retry with backoff
#                     logger.warning(
#                         f"(PDS endpoint: {self.pds_endpoint}): Connection error for DID {did}, retry {retry_count}: {e}"
#                     )
#                     time.sleep(1 * (2**retry_count))  # Exponential backoff
#                 else:
#                     # max retries already reached, move to deadletter.
#                     logger.error(
#                         f"(PDS endpoint: {self.pds_endpoint}): Failed processing DID {did} after {max_retries} retries: {e}"
#                     )
#                     self.temp_deadletter_queue.put({"did": did, "content": ""})
#                     pdm.BACKFILL_DID_STATUS.labels(
#                         endpoint=self.pds_endpoint, status="network_error"
#                     ).inc()
#                     pdm.BACKFILL_QUEUE_SIZES.labels(
#                         endpoint=self.pds_endpoint, queue_type="deadletter"
#                     ).set(self.temp_deadletter_queue.qsize())
#             except Exception as e:
#                 pdm.BACKFILL_ERRORS.labels(endpoint=self.pds_endpoint).inc()
#                 # Other unexpected errors
#                 logger.error(
#                     f"(PDS endpoint: {self.pds_endpoint}): Unexpected error processing DID {did}: {e}"
#                 )
#                 self.temp_deadletter_queue.put({"did": did, "content": ""})
#                 pdm.BACKFILL_DID_STATUS.labels(
#                     endpoint=self.pds_endpoint, status="unexpected_error"
#                 ).inc()
#                 pdm.BACKFILL_QUEUE_SIZES.labels(
#                     endpoint=self.pds_endpoint, queue_type="deadletter"
#                 ).set(self.temp_deadletter_queue.qsize())
#             finally:
#                 pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).dec()

#                 # Track both the summary and histogram for latency
#                 request_latency = time.perf_counter() - start
#                 pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
#                     request_latency
#                 )
#                 pdm.BACKFILL_LATENCY_HISTOGRAM.labels(
#                     endpoint=self.pds_endpoint
#                 ).observe(request_latency)

#     def flush_to_db(self):
#         results = []
#         deadletters = []
#         current_timestamp = datetime.now(timezone.utc).strftime(timestamp_format)
#         while not self.temp_results_queue.empty():
#             # results.append(self.temp_results_queue.get())
#             temp_result = self.temp_results_queue.get()
#             records = get_records_from_pds_bytes(temp_result["content"])
#             records = [
#                 record for record in records if validate_is_valid_bsky_type(record)
#             ]
#             results.extend(records)
#         while not self.temp_deadletter_queue.empty():
#             deadletters.append(self.temp_deadletter_queue.get())
#         logger.info(
#             f"Flushing {len(results)} results and {len(deadletters)} deadletters to DB..."
#         )
#         if len(results) > 0:
#             self.output_results_queue.batch_add_items_to_queue(
#                 items=results, metadata={"timestamp": current_timestamp}
#             )
#         if len(deadletters) > 0:
#             self.output_deadletter_queue.batch_add_items_to_queue(
#                 items=deadletters, metadata={"timestamp": current_timestamp}
#             )

#     def run(self):
#         for did in self.dids:
#             self.temp_work_queue.put(did)
#         logger.info(f"Processing {len(self.dids)} DIDs for {self.pds_endpoint}...")
#         current_idx = 0
#         log_batch_interval = 50
#         total_dids = len(self.dids)

#         total_threads = 8  # TODO: prob increase?
#         latest_batch = []

#         total_dids_processed = 0

#         while not self.temp_work_queue.empty():
#             latest_batch = []
#             # Get a batch of DIDs to process
#             for _ in range(total_threads):
#                 if not self.temp_work_queue.empty():
#                     latest_batch.append(self.temp_work_queue.get())
#                     total_dids_processed += 1
#                 else:
#                     break

#             if current_idx % log_batch_interval == 0:
#                 logger.info(f"Processing DID {current_idx}/{total_dids}...")

#             # if current_idx % total_batches == 0:
#             #     logger.info(
#             #         f"Processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
#             #     )

#             # Process DIDs in parallel using ThreadPoolExecutor
#             with concurrent.futures.ThreadPoolExecutor(
#                 max_workers=total_threads
#             ) as executor:
#                 futures = [
#                     executor.submit(self._process_did, did) for did in latest_batch
#                 ]
#                 # Wait for all threads to complete
#                 concurrent.futures.wait(futures)

#             if (
#                 total_dids_processed > 0
#                 and total_dids_processed % default_write_batch_size == 0
#             ):
#                 logger.info(
#                     f"Total DIDs processed so far: {total_dids_processed}. Flushing to DB."
#                 )
#                 self.flush_to_db()
#                 logger.info(
#                     f"Total DIDs processed so far: {total_dids_processed}. Flushed to DB."
#                 )

#             if current_idx % log_batch_interval == 0:
#                 logger.info(f"Completed processing {current_idx}/{total_dids} DIDs...")
#             # if current_idx % total_batches == 0:
#             #     logger.info(
#             #         f"Completed processing batch {current_idx}/{total_batches} of {total_dids} DIDs..."
#             #     )
#             current_idx += 1

#         logger.info("Finished processing all DIDs.")


# class PDSEndpointWorkerAsynchronous:
#     """Async implementation of PDSEndpointWorker."""

#     def __init__(self, pds_endpoint: str, dids: list[str]):
#         self.pds_endpoint = pds_endpoint
#         self.dids = dids
#         self.token_bucket = AsyncTokenBucket(
#             max_tokens=MANUAL_RATE_LIMIT, window_seconds=300
#         )
#         self.batch_size = 20
#         self.did_batches = create_batches(
#             batch_list=self.dids, batch_size=self.batch_size
#         )

#         self.concurrency = 10

#         # for testing purposes.
#         # self.max_batches = 5
#         # self.max_threads = 8

#         # this leads to ~200, but stalls after ~110 DIDs.
#         self.max_threads = 5
#         self.max_batches = 2 * self.max_threads

#         # ~240. Let's see if it stalls.
#         # self.max_threads = 3
#         # self.max_batches = 4 * self.max_threads

#         self.did_batches = self.did_batches[: self.max_batches]

#         # TODO: copy the DIDs.
#         # breakpoint()

#         logger.info(f"Total batches: {len(self.did_batches)}")

#         # queues:
#         self.temp_results_queue = queue.Queue()
#         self.temp_deadletter_queue = queue.Queue()
#         self.temp_work_queue = queue.Queue()

#         # put batches into queue, so each item in the queue is a batch of DIDs.
#         for batch in self.did_batches:
#             self.temp_work_queue.put(batch)

#         # NOTE: split each batch across multiple threads?
#         self.in_flight = 0
#         self.max_in_flight = 0
#         self.request_times = []
#         self.completed_requests = 0

#     async def _process_did(
#         self,
#         did: str,
#         session: aiohttp.ClientSession,
#         semaphore: asyncio.Semaphore,
#     ) -> requests.Response:
#         # pdm.BACKFILL_REQUESTS.labels(endpoint=self.pds_endpoint).inc()
#         # pdm.BACKFILL_INFLIGHT.labels(endpoint=self.pds_endpoint).inc()
#         did = "did:plc:4rlh46czb2ix4azam3cfyzys"
#         self.in_flight += 1
#         self.max_in_flight = max(self.max_in_flight, self.in_flight)
#         logger.info(f"Starting request for {did}. In-flight: {self.in_flight}")

#         await self.token_bucket._acquire()

#         # make the request and time.
#         request_start = time.perf_counter()
#         async with semaphore:
#             response = await async_send_request_to_pds(
#                 did=did, pds_endpoint=self.pds_endpoint, session=session
#             )
#             _ = await response.read()
#             self.completed_requests += 1
#         request_latency = time.perf_counter() - request_start
#         logger.info(f"Request latency: {request_latency} seconds")
#         # pdm.BACKFILL_LATENCY_SECONDS.labels(endpoint=self.pds_endpoint).observe(
#         #     request_latency
#         # )
#         self.request_times.append(request_latency)
#         headers = response.headers
#         rate_limit_remaining = headers.get("ratelimit-remaining")
#         logger.info(f"Rate limit remaining: {rate_limit_remaining}")
#         return response

#     async def run(self):
#         times = []
#         conn = aiohttp.TCPConnector(
#             limit=self.max_threads, limit_per_host=self.max_threads
#         )
#         # timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_connect=5, sock_read=15)
#         # async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
#         start_time = time.perf_counter()
#         async with aiohttp.ClientSession(connector=conn) as session:
#             idx = 0
#             while idx < len(self.did_batches):
#                 # while not self.temp_work_queue.empty():
#                 semaphore = asyncio.Semaphore(self.concurrency)
#                 time_start = time.perf_counter()
#                 # Create tasks for each DID in the batch to process them concurrently
#                 tasks = []
#                 # latest_batch = self.temp_work_queue.get()
#                 latest_batch = self.did_batches[idx]
#                 idx += 1

#                 async def limited_process(did):
#                     async with semaphore:
#                         return await self._process_did(
#                             did=did, session=session, semaphore=semaphore
#                         )

#                 tasks = [limited_process(did) for did in latest_batch]

#                 # for did in latest_batch:
#                 #     task = asyncio.create_task(limited_process(did))
#                 #     tasks.append(task)

#                 # Wait for all tasks to complete
#                 responses = await asyncio.gather(*tasks)
#                 logger.info(
#                     f"Total results: {len(responses)} ({len(latest_batch)} DIDs across {len(responses)} tasks)"
#                 )
#                 time_end = time.perf_counter()
#                 diff = time_end - time_start
#                 times.append(diff)
#                 # to simulate DB flush.
#                 # logger.info(f"Sleeping for 30 seconds to simulate DB flush...")
#                 # await asyncio.sleep(30)
#                 # logger.info(f"Done sleeping. Continuing...")
#                 # logger.info(f"Sleeping for 15 seconds to simulate DB flush...")
#                 # await asyncio.sleep(15)
#                 # logger.info(f"Done sleeping. Continuing...")

#             # for batch in self.did_batches:
#             #     start = time.perf_counter()
#             #     res: list[requests.Response] = await self._process_batch(batch=batch, session=session)
#             #     logger.info(f"Total results: {len(res)}")
#             #     times.append(time.perf_counter() - start)
#         elapsed = time.perf_counter() - start_time
#         final_qps = self.completed_requests / elapsed
#         logger.info("=== TEST RESULTS ===")
#         logger.info(f"Total requests: {self.completed_requests}")
#         logger.info(f"Time elapsed: {elapsed:.2f} seconds")
#         logger.info(f"Average QPS: {final_qps:.2f}")
#         logger.info(
#             f"Average time to send/receive requests: {np.mean(self.request_times):.3f}s"
#         )
#         logger.info(f"Max in-flight requests: {self.max_in_flight}")
#         logger.info(f"Average time to process each batch: {sum(times)/len(times):.3f}s")
#         # logger.info(f"Average time per threaded batch (so {self.max_threads} threads, each with 1 batch): {sum(times)/len(times):.3f}s")

#         # TODO: add flush db later.


# def filter_previous_did_endpoint_tuples(
#     did_endpoint_tuples: list[tuple[str, str]],
#     results_db: Queue,
#     deadletter_db: Queue,
# ) -> list[tuple[str, str]]:
#     """Load previous results from queues and filter out DIDs that have already been processed."""
#     query_results = results_db.load_dict_items_from_queue()
#     query_deadletter = deadletter_db.load_dict_items_from_queue()
#     query_result_dids = [result["did"] for result in query_results]
#     query_deadletter_dids = [deadletter["did"] for deadletter in query_deadletter]
#     previously_processed_dids = set(query_result_dids) | set(query_deadletter_dids)
#     filtered_did_endpoint_tuples = [
#         did_endpoint_tuple
#         for did_endpoint_tuple in did_endpoint_tuples
#         if did_endpoint_tuple[0] not in previously_processed_dids
#     ]
#     return filtered_did_endpoint_tuples


# def get_write_queues(pds_endpoint: str):
#     # Extract the hostname from the PDS endpoint URL
#     # eg., https://lepista.us-west.host.bsky.network.db -> lepista.us-west.host.bsky.network.db
#     pds_hostname = (
#         pds_endpoint.replace("https://", "").replace("http://", "").replace("/", "")
#     )
#     logger.info(f"Instantiating queues for PDS hostname: {pds_hostname}")

#     output_results_db_path = os.path.join(current_dir, f"results_{pds_hostname}.db")
#     output_deadletter_db_path = os.path.join(
#         current_dir, f"deadletter_{pds_hostname}.db"
#     )

#     output_results_queue = Queue(
#         queue_name=f"results_{pds_hostname}",
#         create_new_queue=True,
#         temp_queue=True,
#         temp_queue_path=output_results_db_path,
#     )
#     output_deadletter_queue = Queue(
#         queue_name=f"deadletter_{pds_hostname}",
#         create_new_queue=True,
#         temp_queue=True,
#         temp_queue_path=output_deadletter_db_path,
#     )
#     return {
#         "output_results_queue": output_results_queue,
#         "output_deadletter_queue": output_deadletter_queue,
#     }

