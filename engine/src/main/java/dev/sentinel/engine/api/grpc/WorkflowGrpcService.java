package dev.sentinel.engine.api.grpc;

import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.domain.WorkflowDefinition;
import dev.sentinel.engine.error.PayloadTooLargeException;
import dev.sentinel.engine.infra.GrpcProperties;
import dev.sentinel.engine.service.FailureQueryService;
import dev.sentinel.engine.service.WorkflowQueryService;
import dev.sentinel.engine.service.WorkflowSubmissionService;
import dev.sentinel.proto.v1.GetTaskFailuresRequest;
import dev.sentinel.proto.v1.GetTaskFailuresResponse;
import dev.sentinel.proto.v1.GetWorkflowStatusRequest;
import dev.sentinel.proto.v1.ListDeadLetterTasksRequest;
import dev.sentinel.proto.v1.ListDeadLetterTasksResponse;
import dev.sentinel.proto.v1.GetWorkflowStatusResponse;
import dev.sentinel.proto.v1.SubmitWorkflowRequest;
import dev.sentinel.proto.v1.SubmitWorkflowResponse;
import dev.sentinel.proto.v1.WorkflowServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Transport for workflow authors.
 *
 * <p>Strictly a translation layer. It converts wire messages to domain types, calls one service,
 * converts the answer back, and does nothing else. Every business decision, including which
 * submissions are valid and which are refused, lives behind {@link WorkflowSubmissionService}, so
 * that adding a second transport later would not mean reimplementing any of it.
 *
 * <p>Errors are not caught here. They propagate to {@link GrpcErrors}, which is the single place
 * that decides what a given failure means to a remote caller.
 */
@Service
public class WorkflowGrpcService extends WorkflowServiceGrpc.WorkflowServiceImplBase {

    private final WorkflowSubmissionService submissionService;
    private final WorkflowQueryService queryService;
    private final FailureQueryService failureQueryService;
    private final GrpcProperties properties;

    public WorkflowGrpcService(
            WorkflowSubmissionService submissionService,
            WorkflowQueryService queryService,
            FailureQueryService failureQueryService,
            GrpcProperties properties) {
        this.submissionService = submissionService;
        this.queryService = queryService;
        this.failureQueryService = failureQueryService;
        this.properties = properties;
    }

    @Override
    public void submitWorkflow(SubmitWorkflowRequest request, StreamObserver<SubmitWorkflowResponse> observer) {
        WorkflowDefinition definition = ProtoMappers.toDefinition(request);
        definition.tasks().forEach(task -> requireWithinPayloadLimit("task '" + task.name() + "' input",
                task.input()));

        WorkflowSubmissionService.SubmissionResult result = submissionService.submit(definition);
        Workflow workflow = result.workflow();

        observer.onNext(SubmitWorkflowResponse.newBuilder()
                .setWorkflowId(workflow.id().toString())
                .setStatus(statusOf(workflow))
                .setDeduplicated(result.deduplicated())
                .build());
        observer.onCompleted();
    }

    @Override
    public void getWorkflowStatus(
            GetWorkflowStatusRequest request, StreamObserver<GetWorkflowStatusResponse> observer) {

        UUID workflowId = ProtoMappers.toUuid(request.getWorkflowId(), "workflow_id");
        WorkflowQueryService.WorkflowView view = queryService.findById(workflowId);

        observer.onNext(ProtoMappers.toStatusResponse(view.workflow(), view.tasks()));
        observer.onCompleted();
    }

    @Override
    public void getTaskFailures(
            GetTaskFailuresRequest request, StreamObserver<GetTaskFailuresResponse> observer) {

        UUID taskId = ProtoMappers.toUuid(request.getTaskId(), "task_id");

        GetTaskFailuresResponse.Builder response = GetTaskFailuresResponse.newBuilder()
                .setTaskId(taskId.toString());
        failureQueryService.failuresOf(taskId).stream()
                .map(ProtoMappers::toFailureRecord)
                .forEach(response::addFailures);

        observer.onNext(response.build());
        observer.onCompleted();
    }

    @Override
    public void listDeadLetterTasks(
            ListDeadLetterTasksRequest request, StreamObserver<ListDeadLetterTasksResponse> observer) {

        java.util.Optional<UUID> workflowId = request.hasWorkflowId()
                ? java.util.Optional.of(ProtoMappers.toUuid(request.getWorkflowId(), "workflow_id"))
                : java.util.Optional.empty();

        ListDeadLetterTasksResponse.Builder response = ListDeadLetterTasksResponse.newBuilder();
        failureQueryService.deadLetters(workflowId, request.getLimit()).stream()
                .map(ProtoMappers::toDeadLetterTask)
                .forEach(response::addTasks);

        observer.onNext(response.build());
        observer.onCompleted();
    }

    private static dev.sentinel.proto.v1.WorkflowStatus statusOf(Workflow workflow) {
        return switch (workflow.status()) {
            case RUNNING -> dev.sentinel.proto.v1.WorkflowStatus.WORKFLOW_STATUS_RUNNING;
            case COMPLETED -> dev.sentinel.proto.v1.WorkflowStatus.WORKFLOW_STATUS_COMPLETED;
            case FAILED -> dev.sentinel.proto.v1.WorkflowStatus.WORKFLOW_STATUS_FAILED;
        };
    }

    /**
     * Rejects oversized payloads at the boundary rather than at the database.
     *
     * <p>The limit is measured in UTF-8 bytes, not characters, because that is what the column and
     * the wire actually hold. A String of 200,000 characters can be well over the byte limit.
     */
    private void requireWithinPayloadLimit(String field, String payload) {
        int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > properties.maxPayloadBytes()) {
            throw new PayloadTooLargeException(field, bytes, properties.maxPayloadBytes());
        }
    }
}
