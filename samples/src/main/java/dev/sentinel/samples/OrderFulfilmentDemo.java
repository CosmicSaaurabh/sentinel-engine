package dev.sentinel.samples;

import dev.sentinel.proto.v1.GetWorkflowStatusRequest;
import dev.sentinel.proto.v1.GetWorkflowStatusResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.SubmitWorkflowResponse;
import dev.sentinel.proto.v1.TaskDefinition;
import dev.sentinel.proto.v1.TaskSummary;
import dev.sentinel.proto.v1.WorkflowServiceGrpc;
import dev.sentinel.proto.v1.WorkflowStatus;
import dev.sentinel.worker.SentinelWorker;
import dev.sentinel.worker.TaskResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A runnable demonstration of the whole system.
 *
 * <p>Submits an order-fulfilment workflow, starts a worker that executes it, and prints what
 * happened. Run it against a live engine:
 *
 * <pre>
 *   docker compose -f infra/docker-compose.yml up -d
 *   ./mvnw -pl engine spring-boot:run
 *   ./mvnw -pl samples -am exec:java -Dexec.mainClass=dev.sentinel.samples.OrderFulfilmentDemo
 * </pre>
 *
 * <p>The DAG is a diamond, because that is the shape where a workflow engine earns its keep:
 *
 * <pre>
 *                 reserve-stock
 *                /             \
 *      charge-card             notify-warehouse
 *                \             /
 *                  send-receipt
 * </pre>
 *
 * <p>{@code charge-card} and {@code notify-warehouse} have no dependency on each other, so they run
 * concurrently, and {@code send-receipt} waits for both. Getting that ordering right by hand, while
 * also surviving a worker dying halfway through, is the problem Sentinel exists to solve.
 *
 * <p>{@code charge-card} fails on its first attempt on purpose, so a run shows the retry path
 * rather than only the happy one.
 */
public final class OrderFulfilmentDemo {

    private static final String TASK_TYPE = "demo.fulfilment";
    private static final String DEFAULT_ENGINE = "localhost:9090";

    private OrderFulfilmentDemo() {
    }

    public static void main(String[] args) throws Exception {
        String engineTarget = args.length > 0 ? args[0] : DEFAULT_ENGINE;
        System.out.printf("Connecting to engine at %s%n", engineTarget);

        ManagedChannel channel = ManagedChannelBuilder.forTarget(engineTarget).usePlaintext().build();
        WorkflowServiceGrpc.WorkflowServiceBlockingStub workflows = WorkflowServiceGrpc.newBlockingStub(channel);

        try {
            String workflowId = submitOrder(workflows);
            System.out.printf("Submitted workflow %s%n%n", workflowId);

            try (SentinelWorker worker = buildWorker(engineTarget)) {
                worker.start();
                GetWorkflowStatusResponse finalStatus = awaitCompletion(workflows, workflowId);
                printSummary(finalStatus);
            }
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static String submitOrder(WorkflowServiceGrpc.WorkflowServiceBlockingStub workflows) {
        SubmitWorkflowResponse response = workflows.submitWorkflow(SubmitWorkflowRequest.newBuilder()
                .setName("order-fulfilment")
                // A submission key makes this safe to retry: run the demo twice with the same key
                // and the second run joins the first execution instead of starting a new one.
                .setSubmissionKey("demo-order-" + Instant.now().toEpochMilli())
                .addAllTasks(orderDag())
                .build());

        return response.getWorkflowId();
    }

    private static List<TaskDefinition> orderDag() {
        return List.of(
                task("reserve-stock", "{\"sku\":\"WIDGET-1\",\"quantity\":2}"),
                task("charge-card", "{\"amountCents\":4999}", "reserve-stock"),
                task("notify-warehouse", "{\"warehouse\":\"LON-3\"}", "reserve-stock"),
                task("send-receipt", "{\"email\":\"customer@example.com\"}", "charge-card", "notify-warehouse"));
    }

    private static TaskDefinition task(String name, String input, String... dependsOn) {
        return TaskDefinition.newBuilder()
                .setName(name)
                .setTaskType(TASK_TYPE)
                .setInput(input)
                .addAllDependsOn(List.of(dependsOn))
                .build();
    }

    private static SentinelWorker buildWorker(String engineTarget) {
        return SentinelWorker.builder()
                .engineTarget(engineTarget)
                .workerId("demo-worker")
                .concurrency(4)
                .pollWait(Duration.ofSeconds(5))
                .register(TASK_TYPE, context -> {
                    System.out.printf("  [attempt %d] running %s%n", context.attempt(), context.taskName());

                    // A deliberately flaky step, so a demo run shows the retry path rather than
                    // only the happy one. The engine schedules the retry; nothing here waits.
                    if (context.taskName().equals("charge-card") && context.attempt() == 1) {
                        System.out.println("           payment gateway timed out, will be retried");
                        return TaskResult.failed("payment gateway timed out");
                    }

                    // Stand-in for real work.
                    Thread.sleep(ThreadLocalRandom.current().nextLong(200, 600));

                    System.out.printf("  [attempt %d] finished %s%n", context.attempt(), context.taskName());
                    return TaskResult.completed("{\"step\":\"%s\",\"ok\":true}".formatted(context.taskName()));
                })
                .build();
    }

    private static GetWorkflowStatusResponse awaitCompletion(
            WorkflowServiceGrpc.WorkflowServiceBlockingStub workflows, String workflowId)
            throws InterruptedException {

        Instant deadline = Instant.now().plus(Duration.ofMinutes(2));
        while (Instant.now().isBefore(deadline)) {
            GetWorkflowStatusResponse status = workflows.getWorkflowStatus(
                    GetWorkflowStatusRequest.newBuilder().setWorkflowId(workflowId).build());

            if (status.getStatus() != WorkflowStatus.WORKFLOW_STATUS_RUNNING) {
                return status;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("workflow " + workflowId + " did not finish within two minutes");
    }

    private static void printSummary(GetWorkflowStatusResponse status) {
        System.out.printf("%nWorkflow %s%n", status.getStatus());
        System.out.println("-".repeat(64));
        System.out.printf("%-20s %-14s %-9s %s%n", "TASK", "STATUS", "ATTEMPTS", "OUTPUT");
        for (TaskSummary task : status.getTasksList()) {
            System.out.printf("%-20s %-14s %-9s %s%n",
                    task.getName(),
                    shortStatus(task.getStatus().name()),
                    task.getAttempt() + "/" + task.getMaxAttempts(),
                    task.hasOutput() ? task.getOutput() : "");
        }
        System.out.println("-".repeat(64));
    }

    private static String shortStatus(String status) {
        return status.replace("TASK_STATUS_", "");
    }
}
