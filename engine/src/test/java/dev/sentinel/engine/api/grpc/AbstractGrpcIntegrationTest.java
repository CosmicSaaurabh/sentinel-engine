package dev.sentinel.engine.api.grpc;

import dev.sentinel.engine.AbstractIntegrationTest;
import dev.sentinel.proto.v1.TaskServiceGrpc;
import dev.sentinel.proto.v1.WorkflowServiceGrpc;
import io.grpc.ManagedChannel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.grpc.test.autoconfigure.AutoConfigureTestGrpcTransport;
import org.springframework.grpc.client.GrpcChannelFactory;
import org.springframework.test.context.TestPropertySource;

/**
 * Base for tests that drive the engine over a real gRPC server.
 *
 * <p>In-process rather than over a socket. That is not a shortcut around the transport: the
 * generated stubs, the serialisation, the interceptors, the exception mapping and the real service
 * implementations are all exercised. What is skipped is the operating system's network stack, which
 * contributes nothing to what these tests check and does contribute port collisions on a busy CI
 * machine.
 *
 * <p>{@code @AutoConfigureTestGrpcTransport} swaps the Netty server and channel factories for
 * in-process ones. Everything layered on top, including the service beans, the exception handler
 * and the virtual-thread executor, is the production wiring.
 */
@AutoConfigureTestGrpcTransport
@TestPropertySource(properties = {
        // A short retry interval keeps the long-poll tests quick without changing what they prove.
        "sentinel.grpc.poll-retry-interval=50ms"
})
abstract class AbstractGrpcIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private GrpcChannelFactory channelFactory;

    private ManagedChannel channel;

    protected WorkflowServiceGrpc.WorkflowServiceBlockingStub workflowStub;
    protected TaskServiceGrpc.TaskServiceBlockingStub taskStub;

    @BeforeEach
    void openChannel() {
        channel = channelFactory.createChannel("engine");
        workflowStub = WorkflowServiceGrpc.newBlockingStub(channel);
        taskStub = TaskServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
        channel.shutdownNow();
        // A leaked channel keeps transport threads alive and turns a passing suite into a build
        // that never exits.
        channel.awaitTermination(10, TimeUnit.SECONDS);
    }
}
