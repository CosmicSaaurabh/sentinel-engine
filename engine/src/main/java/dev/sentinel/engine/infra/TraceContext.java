package dev.sentinel.engine.infra;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.util.Optional;

/**
 * Reads the current W3C trace context so it can be handed to a worker with its task.
 *
 * <h2>What is and is not propagated</h2>
 *
 * <p>The engine traces its own work with Micrometer Tracing, and a task carries its workflow's
 * {@code traceparent} to the worker in {@code AssignedTask.trace_context}.
 *
 * <p>It travels in the message rather than in gRPC metadata deliberately. Metadata would propagate
 * the trace of the <em>poll call</em>, and a poll is not a thing anyone wants to trace: it can
 * return several tasks belonging to several unrelated workflows, and it may have spent twenty
 * seconds waiting for work that has nothing to do with the worker that eventually got it. Attaching
 * the context to the task means the worker joins the trace of the workflow it is actually running.
 *
 * <p><strong>The worker does not automatically emit spans.</strong> The SDK carries no tracing
 * library, because it carries no framework at all, so what it does is expose the context and put
 * the trace id in the logging context. An application that brings its own OpenTelemetry can
 * continue the trace from there. That boundary is a consequence of the SDK's dependency rule rather
 * than an oversight, and it is stated here so nobody looks for spans that were never emitted.
 */
public final class TraceContext {

    /** W3C traceparent, version 00, with the sampled flag set. */
    private static final String TRACEPARENT_FORMAT = "00-%s-%s-01";

    private TraceContext() {
    }

    /**
     * @return the current traceparent, or empty when nothing is being traced, which is normal for
     *         work started by a background loop rather than by a request
     */
    public static Optional<String> currentTraceParent(Tracer tracer) {
        if (tracer == null) {
            return Optional.empty();
        }
        Span span = tracer.currentSpan();
        if (span == null) {
            return Optional.empty();
        }
        io.micrometer.tracing.TraceContext context = span.context();
        return Optional.of(TRACEPARENT_FORMAT.formatted(context.traceId(), context.spanId()));
    }
}
