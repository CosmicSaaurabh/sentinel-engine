package dev.sentinel.engine.service;

import dev.sentinel.engine.error.AdmissionRejectedException;
import dev.sentinel.engine.infra.AdmissionProperties;
import dev.sentinel.engine.repository.CapacityCounterRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Service;

/**
 * Refuses new workflows when the fleet is already saturated.
 *
 * <p>This is the engine's outermost brake, and the reasoning behind refusing rather than queueing is
 * worth being explicit about. If the engine accepted every submission it would still only run as
 * many tasks as it has workers; the surplus would sit in the database growing. From the caller's
 * side that looks like success, so callers keep submitting, and the backlog grows faster than it
 * drains until the queue itself becomes the problem. Refusing is the only answer that gives the
 * caller back the information it needs to slow down.
 *
 * <p>Refusing costs nothing in durability terms: nothing was acknowledged, so no promise is broken.
 * That is why NFR5's guarantee is scoped to submissions that were actually accepted.
 */
@Service
public class AdmissionController {

    private final CapacityCounterRepository capacity;
    private final AdmissionProperties properties;
    private final Counter admitted;
    private final Counter rejected;

    public AdmissionController(
            CapacityCounterRepository capacity, AdmissionProperties properties, MeterRegistry meterRegistry) {
        this.capacity = capacity;
        this.properties = properties;
        this.admitted = Counter.builder("sentinel.admission.admitted")
                .description("workflow submissions accepted")
                .register(meterRegistry);
        this.rejected = Counter.builder("sentinel.admission.rejected")
                .description("workflow submissions refused because the fleet is saturated")
                .register(meterRegistry);
    }

    /**
     * @throws AdmissionRejectedException carrying a jittered retry hint
     */
    public void requireCapacity() {
        if (!properties.enabled()) {
            admitted.increment();
            return;
        }

        long inFlight = capacity.totalInFlight();
        // Rejected at exactly zero free slots, with no safety buffer above it.
        //
        // A buffer was considered and rejected. The counter is already approximate: it can drift
        // when an engine dies between claiming a task and updating it, and it is read a moment
        // before the write it guards. A buffer would be a second, arbitrary fudge stacked on top of
        // an existing imprecision, and its only effect would be to refuse work the fleet could
        // actually have run. The honest answer to "is there room" is "are all the slots taken".
        if (inFlight >= properties.totalSlots()) {
            rejected.increment();
            throw new AdmissionRejectedException(inFlight, properties.totalSlots(), jitteredRetryAfter());
        }
        admitted.increment();
    }

    /**
     * Spreads rejected callers over a window rather than telling all of them to return at the same
     * instant.
     *
     * <p>A fixed retry hint turns a saturated engine into a metronome: every client rejected in the
     * same second comes back in the same later second, and the engine is hit by a wave precisely
     * while it is trying to drain. Jittering between half and one and a half times the configured
     * hint spreads the return.
     */
    private Duration jitteredRetryAfter() {
        long baseMillis = properties.retryAfter().toMillis();
        long half = Math.max(1, baseMillis / 2);
        return Duration.ofMillis(half + ThreadLocalRandom.current().nextLong(baseMillis + 1));
    }
}
