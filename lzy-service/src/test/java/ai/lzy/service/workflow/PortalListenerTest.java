package ai.lzy.service.workflow;

import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.service.PortalSlotsListener;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsGrpc;
import ai.lzy.v1.workflow.LWFS;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PortalListenerTest {

    private static class FsMock extends LzyFsGrpc.LzyFsImplBase {
        private final boolean waitForever;

        private FsMock(boolean waitForever) {
            this.waitForever = waitForever;
        }

        @Override
        public void openOutputSlot(LzyFsApi.SlotRequest request, StreamObserver<LzyFsApi.Message> responseObserver) {
            if (waitForever) {
                try {
                    Thread.sleep(10000);
                    return;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            for (int i = 0; i < 10; i++) {
                responseObserver.onNext(LzyFsApi.Message.newBuilder()
                    .setChunk(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build());
            }

            responseObserver.onNext(LzyFsApi.Message.newBuilder()
                .setControl(LzyFsApi.Message.Controls.EOS)
                .build());

            responseObserver.onCompleted();
        }
    }

    private static class ObserverMock implements StreamObserver<LWFS.ReadStdSlotsResponse> {
        private final CompletableFuture<Throwable> completed = new CompletableFuture<>();
        private final List<LWFS.ReadStdSlotsResponse> responses = new ArrayList<>();


        @Override
        public void onNext(LWFS.ReadStdSlotsResponse readStdSlotsResponse) {
            responses.add(readStdSlotsResponse);
        }

        @Override
        public void onError(Throwable throwable) {
            completed.complete(throwable);
        }

        @Override
        public void onCompleted() {
            completed.complete(null);
        }

        public List<LWFS.ReadStdSlotsResponse> getResponses() {
            return responses;
        }

        public CompletableFuture<Throwable> getCompleted() {
            return completed;
        }
    }

    @Test
    public void simple() throws IOException, ExecutionException, InterruptedException {
        var service = new FsMock(false);
        var port = FreePortFinder.find(10000, 20000);

        var server = ServerBuilder.forPort(port)
            .addService(service)
            .build();

        server.start();

        var address = HostAndPort.fromParts("localhost", port);

        var observer = new ObserverMock();

        new PortalSlotsListener(address, "portal-id", observer);

        var t = observer.getCompleted().get();
        Assert.assertNull(t);
        server.shutdownNow();
    }

    @Test
    public void cancel() throws IOException, ExecutionException, InterruptedException {
        var service = new FsMock(true);
        var port = FreePortFinder.find(10000, 20000);

        var server = ServerBuilder.forPort(port)
                .addService(service)
                .build();

        server.start();

        var address = HostAndPort.fromParts("localhost", port);

        var observer = new ObserverMock();

        var listener = new PortalSlotsListener(address, "portal-id", observer);
        listener.cancel("Cancelled");

        var t = observer.getCompleted().get();

        Assert.assertNotNull(t);
        Assert.assertSame(Status.Code.CANCELLED, ((StatusException) t).getStatus().getCode());
        server.shutdownNow();
    }
}
