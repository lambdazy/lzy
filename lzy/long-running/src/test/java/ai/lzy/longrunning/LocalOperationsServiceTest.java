package ai.lzy.longrunning;

import ai.lzy.v1.longrunning.LongRunning;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;


public class LocalOperationsServiceTest {
    private LocalOperationService opService;

    @Before
    public void setUp() {
        opService = new LocalOperationService("common");
    }

    @After
    public void tearDown() {
        opService = null;
    }

    @Test
    public void correctnessInsertTest() throws InterruptedException {
        int n = 10;
        var readyLatch = new CountDownLatch(n);
        var doneLatch = new CountDownLatch(n);
        var executor = Executors.newFixedThreadPool(n);
        var failed = new AtomicBoolean(false);

        var ikAndId = new ArrayList<String>(Collections.nCopies(n, null));

        for (int i = 0; i < n; ++i) {
            var index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var op = opService.registerOperation(
                        Operation.createCompleted(
                            String.valueOf(index),
                            /* creator */ "test",
                            /* desc */ "simple-op",
                            new Operation.IdempotencyKey(String.valueOf(index), "hash"),
                            /* meta */ null,
                            Any.getDefaultInstance()));

                    ikAndId.set(index, op.id());
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        assertFalse(failed.get());
        for (int i = 0; i < n; i++) {
            var byId = opService.get(String.valueOf(i));
            var byIk = opService.getByIdempotencyKey(String.valueOf(i));
            var byIdGrpc = get(String.valueOf(i));

            assertNotNull(byId);
            assertNotNull(byIk);

            assertEquals(String.valueOf(i), ikAndId.get(i));
            assertEquals(byId.id(), byIk.id());
            assertEquals(byId.id(), byIdGrpc.getId());
            assertEquals(byId.id(), ikAndId.get(i));
            assertEquals(byIk.idempotencyKey().token(), ikAndId.get(i));
        }
    }

    @Test
    public void awaitTest() throws InterruptedException {
        var op = opService.registerOperation(Operation.create("test", "simple-op", null, null));
        var syncLabel = new CountDownLatch(1);
        var observer = new BlockingWaitThread(op.id(), Duration.ofSeconds(5), syncLabel);

        observer.start();
        opService.updateResponse(op.id(), Any.getDefaultInstance());

        assertTrue(syncLabel.await(5, TimeUnit.SECONDS));
        assertTrue(observer.isWaited());
    }

    private LongRunning.Operation get(String opId) {
        LongRunning.Operation[] result = {null};
        opService.get(LongRunning.GetOperationRequest.newBuilder().setOperationId(opId).build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LongRunning.Operation operation) {
                    result[0] = operation;
                }

                @Override
                public void onError(Throwable throwable) {}

                @Override
                public void onCompleted() {}
            });
        return result[0];
    }

    private class BlockingWaitThread extends Thread {
        private final String opId;
        private final Duration timeout;
        private final CountDownLatch latch;

        private boolean waited;

        public BlockingWaitThread(String opId, Duration timeout, CountDownLatch latch) {
            this.opId = opId;
            this.timeout = timeout;
            this.latch = latch;
        }

        @Override
        public void run() {
            waited = opService.await(opId, timeout);
            latch.countDown();
        }

        public boolean isWaited() {
            return waited;
        }
    }
}
