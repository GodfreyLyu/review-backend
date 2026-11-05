CREATE TABLE users (
  login_id text PRIMARY KEY, user_id text NOT NULL UNIQUE, password_hash text NOT NULL,
  created_at text NOT NULL, disabled boolean NOT NULL DEFAULT false
);
CREATE TABLE sessions (
  token_hash text PRIMARY KEY, user_id text NOT NULL REFERENCES users(user_id),
  login_id text NOT NULL, csrf_token text NOT NULL, expires_at double precision NOT NULL
);
CREATE INDEX sessions_expiry ON sessions(expires_at);
CREATE TABLE reviews (
  review_id text PRIMARY KEY, user_id text NOT NULL REFERENCES users(user_id),
  client_request_id text NOT NULL, source_code text NOT NULL, language text NOT NULL,
  review_result text, status text NOT NULL DEFAULT 'queued'
    CHECK(status IN ('queued','running','completed','failed')),
  execution_state text NOT NULL DEFAULT 'queued', error_code text, error_message text,
  model_id text, model_revision text, retry_count integer NOT NULL DEFAULT 0,
  attempt integer NOT NULL DEFAULT 0, fence bigint NOT NULL DEFAULT 0,
  lease_owner text, lease_until double precision, available_at double precision NOT NULL,
  deadline_at double precision NOT NULL, created_at double precision NOT NULL,
  updated_at double precision NOT NULL, last_enqueued_at double precision NOT NULL,
  execution_version text NOT NULL DEFAULT 'review-v1', trace_id text NOT NULL,
  job_topic text NOT NULL DEFAULT 'review.jobs.v1', sections jsonb NOT NULL DEFAULT '{}',
  rpc_instance_id text, rpc_request_id text, rpc_section text, rpc_state text,
  last_rpc_state text, recovery_count integer NOT NULL DEFAULT 0,
  UNIQUE(user_id,client_request_id)
);
CREATE INDEX reviews_history ON reviews(user_id,created_at DESC,review_id DESC);
CREATE INDEX reviews_work ON reviews(status,available_at,lease_until);
CREATE TABLE admission_lock (id integer PRIMARY KEY CHECK(id=1));
INSERT INTO admission_lock VALUES (1);
CREATE TABLE inference_slots (slot_id text PRIMARY KEY, review_id text REFERENCES reviews(review_id));
INSERT INTO inference_slots VALUES ('default',NULL);
CREATE TABLE outbox (
  event_id text PRIMARY KEY, review_id text, topic text NOT NULL, payload jsonb NOT NULL,
  available_at double precision NOT NULL, created_at double precision NOT NULL,
  sent_at double precision, claim_owner text, claim_until double precision,
  publish_attempts integer NOT NULL DEFAULT 0, last_error text
);
CREATE INDEX outbox_due ON outbox(available_at,created_at) WHERE sent_at IS NULL;
CREATE TABLE rate_limits (
  scope text NOT NULL, bucket bigint NOT NULL, count integer NOT NULL,
  PRIMARY KEY(scope,bucket)
);
CREATE TABLE kafka_dlq (
  topic text NOT NULL, partition_id integer NOT NULL, offset_id bigint NOT NULL,
  reason text NOT NULL, payload_digest text NOT NULL, created_at double precision NOT NULL,
  PRIMARY KEY(topic,partition_id,offset_id)
);
