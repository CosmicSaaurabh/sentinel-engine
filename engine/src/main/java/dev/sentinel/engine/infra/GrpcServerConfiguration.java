package dev.sentinel.engine.infra;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerExecutorProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Server-side transport configuration.
 *
 * <p>The gRPC server runs handlers on virtual threads, and that choice is what makes long polling
 * viable at all. A waiting poll spends nearly all of its life parked between claim attempts, doing
 * nothing but holding its place. On platform threads the pool would have to be at least as large as
 * the worker fleet, which is the connection-pool problem wearing a different hat: a bounded pool
 * would turn extra workers into an invisible queue, and an unbounded one is banned outright by this
 * project's own rules.
 *
 * <p>Virtual threads remove the thread-count constraint but not the need for a bound. That bound
 * lives in {@code GrpcProperties.maxWaitingPolls} and is enforced explicitly, because "waiting is
 * cheap" is precisely the reasoning that produces an unbounded queue nobody noticed building.
 *
 * <p>The database connection pool is unaffected and remains the real resource limit: a parked poll
 * holds no connection between attempts, only during the brief claim itself.
 */
@Configuration(proxyBeanMethods = false)
public class GrpcServerConfiguration {

    @Bean
    public GrpcServerExecutorProvider virtualThreadGrpcServerExecutorProvider() {
        Executor executor = Executors.newVirtualThreadPerTaskExecutor();
        return () -> executor;
    }
}
