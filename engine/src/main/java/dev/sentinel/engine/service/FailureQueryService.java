package dev.sentinel.engine.service;

import dev.sentinel.engine.domain.RecordedFailure;
import dev.sentinel.engine.error.EntityNotFoundException;
import dev.sentinel.engine.repository.DeadLetterRepository;
import dev.sentinel.engine.repository.DeadLetterRepository.DeadLetterTask;
import dev.sentinel.engine.repository.TaskFailureRepository;
import dev.sentinel.engine.repository.TaskRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

/**
 * Answers "why did this fail?" and "what is broken right now?".
 *
 * <p>Read-only and outside any transaction. These are diagnostic queries whose answer is stale the
 * moment it is sent, so paying isolation costs to make a snapshot internally consistent would buy
 * nothing an operator can use.
 */
@Service
public class FailureQueryService {

    private final TaskFailureRepository failures;
    private final DeadLetterRepository deadLetters;
    private final TaskRepository tasks;

    public FailureQueryService(
            TaskFailureRepository failures, DeadLetterRepository deadLetters, TaskRepository tasks) {
        this.failures = failures;
        this.deadLetters = deadLetters;
        this.tasks = tasks;
    }

    /**
     * The cause chain for one task, oldest attempt first.
     *
     * <p>Existence of the task is checked separately, so that "this task never failed" comes back as
     * an empty list rather than being indistinguishable from "this task id is wrong".
     */
    public List<RecordedFailure> failuresOf(UUID taskId) {
        if (tasks.findById(taskId).isEmpty()) {
            throw EntityNotFoundException.task(taskId);
        }
        return failures.findByTaskId(taskId);
    }

    public List<DeadLetterTask> deadLetters(Optional<UUID> workflowId, int limit) {
        return workflowId
                .map(id -> deadLetters.findByWorkflowId(id, limit))
                .orElseGet(() -> deadLetters.findRecent(limit));
    }
}
