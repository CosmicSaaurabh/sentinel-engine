-- The W3C trace context of the request that submitted the workflow.
--
-- Stored rather than passed along in memory, because the workflow it belongs to outlives the
-- request that created it by minutes or hours. A task claimed twenty minutes after submission still
-- needs to be able to join the trace that started it, and by then the submitting request is long
-- gone from every in-memory context.
--
-- Nullable, because a workflow can be submitted by a caller that is not tracing, or by a background
-- process. An absent trace is normal and must not be treated as an error.
ALTER TABLE workflows ADD COLUMN trace_context text;

COMMENT ON COLUMN workflows.trace_context IS
    'W3C traceparent captured at submission, handed to workers so their spans join this trace';
