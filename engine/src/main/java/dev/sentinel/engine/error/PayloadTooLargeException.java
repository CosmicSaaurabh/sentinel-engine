package dev.sentinel.engine.error;

import java.io.Serial;

/**
 * A task input or output exceeds the configured limit.
 *
 * <p>The limit is far below gRPC's own message cap, and the binding constraint is not the wire. It
 * is that every payload lands in a {@code jsonb} column and is copied again into the outbox, so a
 * workflow shuttling megabyte payloads between steps turns the task table into a blob store and
 * makes the queue slow for every other workflow sharing it.
 *
 * <p>The intended answer for large data is a reference: put the object in storage the client owns,
 * and pass its identifier through the workflow.
 */
public final class PayloadTooLargeException extends SentinelException {

    @Serial
    private static final long serialVersionUID = 1L;

    public PayloadTooLargeException(String field, int actualBytes, int limitBytes) {
        super("%s is %d bytes, which exceeds the %d byte limit; pass a reference to external storage instead"
                .formatted(field, actualBytes, limitBytes));
    }
}
