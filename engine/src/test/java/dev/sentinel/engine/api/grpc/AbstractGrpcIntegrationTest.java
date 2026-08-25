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
import org.springframework.test.annotation.DirtiesContext;
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
 *
 * <h2>Why the context is dirtied after every class</h2>
 *
 * <p>The in-process server address is generated once per JVM and shared by every test context. Two
 * Spring contexts alive at the same time therefore try to serve the same address, and channels can
 * reach the wrong engine, which is backed by a different database container. The symptom is
 * horrible: a test submits a workflow successfully, then waits forever for a worker that is polling
 * a completely different engine, and it only happens when a particular pair of classes runs in the
 * same JVM.
 *
 * <p>Dirtying the context after each class guarantees at most one server is alive at a time. It
 * costs a container per gRPC test class, which is a fair price for a suite whose results do not
 * depend on the order it happens to run in.
 *
 * <p>Any test class that needs different engine properties gets its own context, so it inherits
 * this protection automatically by extending this class rather than having to remember it.
 */
@AutoConfigureTestGrpcTransport
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
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
