package dev.sentinel.engine.infra;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Transport-level policy for the worker protocol.
 *
 * @param maxPollWait ceiling on how long the engine will hold a poll open, whatever the client
 *        asks for. Bounding it server-side means a client cannot pin a waiter indefinitely
 * @param pollRetryInterval how often a waiting poll re-attempts the claim. This is what dispatch
 *        latency is actually made of, and it is also the total query load an idle fleet generates,
 *        so it is the one number to change when tuning that trade-off
 * @param maxWaitingPolls how many polls may be parked at once on this instance. Virtual threads
 *        make waiting cheap, not free, and an unbounded number of cheap waiters is still an
 *        unbounded queue
 * @param waitingPollRetryAfter hint returned when polls are shed, jittered before it is sent
 * @param maxPayloadBytes limit on a task input or output
 */
@ConfigurationProperties("sentinel.grpc")
public record GrpcProperties(
        @DefaultValue("20s") Duration maxPollWait,
        @DefaultValue("250ms") Duration pollRetryInterval,
        @DefaultValue("2000") int maxWaitingPolls,
        @DefaultValue("1s") Duration waitingPollRetryAfter,
        @DefaultValue("262144") int maxPayloadBytes) {

    public GrpcProperties {
        if (maxWaitingPolls < 1) {
            throw new IllegalArgumentException("sentinel.grpc.max-waiting-polls must be at least 1");
        }
        if (pollRetryInterval.isZero() || pollRetryInterval.isNegative()) {
            throw new IllegalArgumentException("sentinel.grpc.poll-retry-interval must be positive");
        }
        if (maxPayloadBytes < 1) {
            throw new IllegalArgumentException("sentinel.grpc.max-payload-bytes must be at least 1");
        }
    }
}
