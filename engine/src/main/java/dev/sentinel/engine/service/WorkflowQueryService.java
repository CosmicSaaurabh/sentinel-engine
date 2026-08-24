package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.Task;
import dev.sentinel.engine.domain.Workflow;
import dev.sentinel.engine.error.EntityNotFoundException;
import dev.sentinel.engine.repository.TaskRepository;
import dev.sentinel.engine.repository.WorkflowRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Reads workflow state for callers asking "how is my workflow doing?" (FR7).
 *
 * <p>No transaction wrapper. Two reads on a single Postgres connection under READ COMMITTED can see
 * two different snapshots, so a workflow could in principle be reported as RUNNING alongside a set
 * of tasks that are all terminal. That skew is accepted rather than fixed with a repeatable-read
 * transaction, because this is a status query whose answer is stale the moment it is sent, and
 * paying isolation costs on a read path to make a snapshot internally consistent buys nothing the
 * caller can use.
 *
 * <p>What would change that: any caller that treats this endpoint as a decision point rather than
 * as information. There is none today, and this comment is here so that adding one is a conscious
 * act.
 */
@Service
public class WorkflowQueryService {

    private final WorkflowRepository workflows;
    private final TaskRepository tasks;

    public WorkflowQueryService(WorkflowRepository workflows, TaskRepository tasks) {
        this.workflows = workflows;
        this.tasks = tasks;
    }

    public WorkflowView findById(UUID workflowId) {
        Workflow workflow = workflows.findById(workflowId)
                .orElseThrow(() -> EntityNotFoundException.workflow(workflowId));
        return new WorkflowView(workflow, tasks.findByWorkflowId(workflowId));
    }

    public record WorkflowView(Workflow workflow, List<Task> tasks) {

        public WorkflowView {
            tasks = List.copyOf(tasks);
        }
    }
}
