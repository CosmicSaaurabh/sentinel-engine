package dev.sentinel.engine.api.grpc;

import dev.sentinel.engine.domain.ClaimedTask;
import dev.sentinel.engine.error.PayloadTooLargeException;
import dev.sentinel.engine.error.PollCapacityExceededException;
import dev.sentinel.engine.infra.GrpcProperties;
import dev.sentinel.engine.repository.WorkerRepository;
import dev.sentinel.engine.service.ClaimRequest;
import dev.sentinel.engine.service.CompletionOutcome;
import dev.sentinel.engine.service.TaskClaimService;
import dev.sentinel.engine.service.TaskCompletionService;
import dev.sentinel.proto.v1.CompleteTaskRequest;
import dev.sentinel.proto.v1.FailTaskRequest;
import dev.sentinel.proto.v1.HeartbeatRequest;
import dev.sentinel.proto.v1.HeartbeatResponse;
import dev.sentinel.proto.v1.PollForTasksRequest;
import dev.sentinel.proto.v1.PollForTasksResponse;
import dev.sentinel.proto.v1.ReportResponse;
import dev.sentinel.proto.v1.TaskLease;
import dev.sentinel.proto.v1.TaskServiceGrpc;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The worker protocol.
 *
 * <p>The interesting part of this class is {@link #pollForTasks}, which is a long poll: rather than
 * answering "nothing available" straight away and letting the worker sleep, the engine holds the
 * request open and keeps trying until either work appears or the caller's wait runs out.
 *
 * <h2>Why hold the request instead of answering immediately</h2>
 *
 * <p>With a plain poll, dispatch latency averages half the worker's poll interval, so meeting a
 * 500ms budget needs roughly a 200ms interval. A thousand idle workers at that interval generate
 * five thousand queries a second asking a question whose answer is almost always no, and the engine
 * spends its database capacity proving there is nothing to do.
 *
 * <p>Holding the request moves the retry cadence to the engine, where one number controls the whole
 * fleet's idle load, and lets a worker be answered the moment work appears rather than on its next
 * scheduled ask.
 *
 * <h2>What holding a request costs, and how it is bounded</h2>
 *
 * <p>A waiting poll occupies a thread. The server runs handlers on virtual threads, so a parked
 * waiter costs memory rather than a platform thread, and no connection is held between attempts.
 *
 * <p>That is exactly the reasoning that builds an unbounded queue by accident, so the number of
 * simultaneous waiters is capped explicitly. Overflow is shed with {@code RESOURCE_EXHAUSTED} and a
 * jittered hint. A shed worker backs off and may reach another instance; a silently queued one just
 * waits, and nothing measures it.
 */
@Service
public class TaskGrpcService extends TaskServiceGrpc.TaskServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(TaskGrpcService.class);

    private final TaskClaimService claimService;
    private final TaskCompletionService completionService;
    private final WorkerRepository workers;
    private final GrpcProperties properties;

    private final Semaphore waitingPolls;
    private final AtomicInteger currentlyWaiting = new AtomicInteger();

    public TaskGrpcService(
            TaskClaimService claimService,
            TaskCompletionService completionService,
            WorkerRepository workers,
            GrpcProperties properties,
            MeterRegistry meterRegistry) {
        this.claimService = claimService;
        this.completionService = completionService;
        this.workers = workers;
        this.properties = properties;
        this.waitingPolls = new Semaphore(properties.maxWaitingPolls());

        Gauge.builder("sentinel.poll.waiting", currentlyWaiting, AtomicInteger::get)
                .description("polls currently parked waiting for work")
                .register(meterRegistry);
    }

    // ------------------------------------------------------------------
    // Long poll
    // ------------------------------------------------------------------

    @Override
    public void pollForTasks(PollForTasksRequest request, StreamObserver<PollForTasksResponse> observer) {
        // Answered before a waiter slot is even taken. A worker that can run nothing has nothing to
        // wait for, and parking it would occupy one of a bounded number of slots to no purpose,
        // which is a cheap way for a misconfigured fleet to shed everyone else's polls.
        if (request.getTaskTypesList().isEmpty()) {
            observer.onNext(PollForTasksResponse.getDefaultInstance());
            observer.onCompleted();
            return;
        }

        if (!waitingPolls.tryAcquire()) {
            throw new PollCapacityExceededException(properties.maxWaitingPolls(), jitteredRetryHint());
        }
        currentlyWaiting.incrementAndGet();
        try {
            observer.onNext(waitForTasks(request));
            observer.onCompleted();
        } finally {
            currentlyWaiting.decrementAndGet();
            waitingPolls.release();
        }
    }

    private PollForTasksResponse waitForTasks(PollForTasksRequest request) {
        recordWorkerLiveness(request);

        ClaimRequest claimRequest = new ClaimRequest(
                request.getWorkerId(),
                request.getTaskTypesList(),
                Math.max(1, request.getBatchSize()));

        long deadlineNanos = System.nanoTime() + effectiveWait(request).toNanos();

        while (true) {
            List<ClaimedTask> claimed = claimService.claim(claimRequest);
            if (!claimed.isEmpty()) {
                return toResponse(claimed);
            }

            // The client giving up is checked every round rather than only at the end. Continuing
            // to query on behalf of a caller that has already hung up is pure waste, and under a
            // reconnect storm it is waste multiplied by the size of the fleet.
            if (Context.current().isCancelled()) {
                log.debug("poll from worker {} was cancelled by the caller", request.getWorkerId());
                return PollForTasksResponse.getDefaultInstance();
            }

            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return PollForTasksResponse.getDefaultInstance();
            }
            if (!sleepBeforeRetry(remainingNanos)) {
                return PollForTasksResponse.getDefaultInstance();
            }
        }
    }

    /**
     * How long this poll may actually wait.
     *
     * <p>Three limits, and the shortest wins: what the client asked for, what the engine permits,
     * and what the call's gRPC deadline leaves. Ignoring the deadline would leave the engine
     * querying for a caller that has already stopped listening.
     *
     * <p>A small margin is taken off the deadline so the response has time to be written. Answering
     * exactly at the deadline reliably produces a cancelled call, which the client cannot tell apart
     * from a real failure.
     */
    private Duration effectiveWait(PollForTasksRequest request) {
        Duration requested = request.hasMaxWait()
                ? Duration.ofSeconds(request.getMaxWait().getSeconds(), request.getMaxWait().getNanos())
                : properties.maxPollWait();

        Duration wait = requested.isNegative() || requested.isZero()
                ? Duration.ZERO
                : min(requested, properties.maxPollWait());

        Deadline deadline = Context.current().getDeadline();
        if (deadline != null) {
            Duration untilDeadline = Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS))
                    .minus(properties.pollRetryInterval());
            wait = untilDeadline.isNegative() ? Duration.ZERO : min(wait, untilDeadline);
        }
        return wait;
    }

    /**
     * Waits before the next claim attempt.
     *
     * <p>Jittered so that a fleet reconnecting after an engine restart does not settle into lockstep
     * and hit the database in synchronised waves. They all failed at the same instant, so without
     * jitter they would all retry at the same instants forever after.
     *
     * @return false if the thread was interrupted, meaning the poll should stop
     */
    private boolean sleepBeforeRetry(long remainingNanos) {
        long intervalNanos = properties.pollRetryInterval().toNanos();
        long jitteredNanos = intervalNanos / 2 + ThreadLocalRandom.current().nextLong(intervalNanos);
        try {
            TimeUnit.NANOSECONDS.sleep(Math.min(jitteredNanos, remainingNanos));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Records that this worker is alive.
     *
     * <p>Best effort on purpose. The registry is for operator visibility and capacity accounting,
     * never for correctness: ownership of a task is proven by its fencing token. A failure to record
     * liveness must therefore never stop a worker from getting work, so it is logged and swallowed
     * rather than propagated.
     */
    private void recordWorkerLiveness(PollForTasksRequest request) {
        try {
            workers.registerOrHeartbeat(
                    request.getWorkerId(),
                    request.getTaskTypesList(),
                    Math.max(1, request.getMaxConcurrency()));
        } catch (RuntimeException e) {
            log.warn("could not record liveness for worker {}; this affects visibility only",
                    request.getWorkerId(), e);
        }
    }

    private static PollForTasksResponse toResponse(List<ClaimedTask> claimed) {
        PollForTasksResponse.Builder response = PollForTasksResponse.newBuilder();
        claimed.stream().map(ProtoMappers::toAssignedTask).forEach(response::addTasks);
        return response.build();
    }

    // ------------------------------------------------------------------
    // Heartbeat
    // ------------------------------------------------------------------

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> observer) {
        Map<UUID, Long> leases = new LinkedHashMap<>();
        for (TaskLease lease : request.getLeasesList()) {
            leases.put(ProtoMappers.toUuid(lease.getTaskId(), "task_id"), lease.getFencingToken());
        }

        TaskCompletionService.HeartbeatResult result =
                completionService.heartbeatAll(request.getWorkerId(), leases);

        HeartbeatResponse.Builder response = HeartbeatResponse.newBuilder();
        result.renewed().forEach(taskId -> response.addRenewedTaskIds(taskId.toString()));
        result.lost().forEach(taskId -> response.addLostTaskIds(taskId.toString()));

        observer.onNext(response.build());
        observer.onCompleted();
    }

    // ------------------------------------------------------------------
    // Reports
    // ------------------------------------------------------------------

    @Override
    public void completeTask(CompleteTaskRequest request, StreamObserver<ReportResponse> observer) {
        requireWithinPayloadLimit("output", request.getOutput());

        CompletionOutcome outcome = completionService.complete(
                ProtoMappers.toUuid(request.getTaskId(), "task_id"),
                request.getWorkerId(),
                request.getFencingToken(),
                request.getOutput().isEmpty() ? "null" : request.getOutput());

        observer.onNext(toReportResponse(outcome));
        observer.onCompleted();
    }

    @Override
    public void failTask(FailTaskRequest request, StreamObserver<ReportResponse> observer) {
        CompletionOutcome outcome = completionService.fail(
                ProtoMappers.toUuid(request.getTaskId(), "task_id"),
                request.getWorkerId(),
                request.getFencingToken(),
                ProtoMappers.toFailureKind(request.getKind()),
                request.getMessage());

        observer.onNext(toReportResponse(outcome));
        observer.onCompleted();
    }

    private static ReportResponse toReportResponse(CompletionOutcome outcome) {
        return ReportResponse.newBuilder()
                .setAlreadyRecorded(outcome == CompletionOutcome.ALREADY_RECORDED)
                .build();
    }

    // ------------------------------------------------------------------

    private void requireWithinPayloadLimit(String field, String payload) {
        int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > properties.maxPayloadBytes()) {
            throw new PayloadTooLargeException(field, bytes, properties.maxPayloadBytes());
        }
    }

    private Duration jitteredRetryHint() {
        long baseMillis = properties.waitingPollRetryAfter().toMillis();
        return Duration.ofMillis(baseMillis / 2 + ThreadLocalRandom.current().nextLong(baseMillis + 1));
    }

    private static Duration min(Duration first, Duration second) {
        return first.compareTo(second) <= 0 ? first : second;
    }
}
