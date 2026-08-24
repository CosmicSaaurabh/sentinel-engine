package dev.sentinel.engine.api.grpc;

import dev.sentinel.engine.error.AdmissionRejectedException;
import dev.sentinel.engine.error.EntityNotFoundException;
import dev.sentinel.engine.error.IllegalTransitionException;
import dev.sentinel.engine.error.InvalidWorkflowException;
import dev.sentinel.engine.error.PayloadTooLargeException;
import dev.sentinel.engine.error.PollCapacityExceededException;
import dev.sentinel.engine.error.StaleFencingTokenException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.grpc.server.exception.GrpcExceptionHandler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

/**
 * Turns engine failures into gRPC statuses.
 *
 * <p>This mapping is the client's entire basis for deciding whether to retry, so getting it wrong
 * produces bugs that look like the engine misbehaving. Two entries carry most of the weight.
 *
 * <p>{@code FAILED_PRECONDITION} for a lost lease must be understood by clients as
 * <em>non-retryable</em>. A worker retrying a rejected completion is arguing with a decision that
 * another worker has already acted on: the task has been re-queued, claimed, and possibly finished
 * elsewhere. The correct response is to abandon it locally and poll for new work.
 *
 * <p>{@code UNAVAILABLE} for a database that cannot be reached is what makes the CP position visible
 * on the wire. The engine is not degrading gracefully or guessing; it is declining to act, and the
 * client should back off and try again rather than treating the call as failed forever.
 */
@Component
public class GrpcErrors implements GrpcExceptionHandler {

    /**
     * How long the client should wait before retrying, in milliseconds.
     *
     * <p>A custom metadata key rather than gRPC's own retry machinery, because the value is
     * computed per rejection from live capacity and is already jittered by the engine.
     */
    public static final Metadata.Key<String> RETRY_AFTER_MILLIS =
            Metadata.Key.of("retry-after-millis", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public StatusException handleException(Throwable exception) {
        return switch (exception) {
            case InvalidWorkflowException e -> Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asException();

            case PayloadTooLargeException e -> Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asException();

            case EntityNotFoundException e -> Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asException();

            case AdmissionRejectedException e -> Status.RESOURCE_EXHAUSTED
                    .withDescription(e.getMessage())
                    .asException(retryAfter(e.retryAfter()));

            case PollCapacityExceededException e -> Status.RESOURCE_EXHAUSTED
                    .withDescription(e.getMessage())
                    .asException(retryAfter(e.retryAfter()));

            case StaleFencingTokenException e -> Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage())
                    .asException();

            case IllegalTransitionException e -> Status.ABORTED
                    .withDescription(e.getMessage())
                    .asException();

            // Both of these mean the same thing to a caller: the engine could not reach the one
            // thing it is allowed to trust. Postgres being unreachable and the connection pool
            // being exhausted are different causes with an identical correct response.
            case CannotCreateTransactionException e -> unavailable(e);
            case DataAccessResourceFailureException e -> unavailable(e);

            case IllegalArgumentException e -> Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asException();

            // Anything unmapped becomes INTERNAL without leaking its message. An unexpected
            // exception's text is written for operators reading logs, not for remote callers.
            default -> Status.INTERNAL
                    .withDescription("internal engine error")
                    .withCause(exception)
                    .asException();
        };
    }

    private static StatusException unavailable(Throwable cause) {
        return Status.UNAVAILABLE
                .withDescription("the engine cannot reach its database and is refusing work rather than guessing")
                .withCause(cause)
                .asException();
    }

    private static Metadata retryAfter(Duration retryAfter) {
        Metadata metadata = new Metadata();
        metadata.put(RETRY_AFTER_MILLIS, Long.toString(retryAfter.toMillis()));
        return metadata;
    }
}
