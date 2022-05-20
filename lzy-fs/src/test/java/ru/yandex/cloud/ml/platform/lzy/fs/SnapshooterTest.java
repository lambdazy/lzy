package ru.yandex.cloud.ml.platform.lzy.fs;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.fs.mock.OutputSlotMock;
import ru.yandex.cloud.ml.platform.lzy.fs.mock.ServiceMock;
import ru.yandex.cloud.ml.platform.lzy.snapshot.*;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public class SnapshooterTest {

    @Test
    public void testSnapshooterAbort() throws IOException, InterruptedException {

        // Setup
        AtomicBoolean suspended = new AtomicBoolean(false);
        AtomicBoolean aborted = new AtomicBoolean(false);

        SnapshotApiGrpc.SnapshotApiImplBase impl = new SnapshotApiGrpc.SnapshotApiImplBase() {
            @Override
            public void abort(LzyWhiteboard.AbortCommand request,
                              StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
                aborted.set(true);
                responseObserver.onNext(LzyWhiteboard.OperationStatus.newBuilder()
                        .setStatus(LzyWhiteboard.OperationStatus.Status.OK).build());
                responseObserver.onCompleted();
            }

            @Override
            public void prepareToSave(LzyWhiteboard.PrepareCommand request,
                    StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
                responseObserver.onNext(LzyWhiteboard.OperationStatus.newBuilder()
                        .setStatus(LzyWhiteboard.OperationStatus.Status.OK).build());
                responseObserver.onCompleted();
            }

            @Override
            public void commit(LzyWhiteboard.CommitCommand request,
                    StreamObserver<LzyWhiteboard.OperationStatus> responseObserver) {
                responseObserver.onNext(LzyWhiteboard.OperationStatus.newBuilder()
                        .setStatus(LzyWhiteboard.OperationStatus.Status.OK).build());
                responseObserver.onCompleted();
            }
        };

        ServiceMock<SnapshotApiGrpc.SnapshotApiImplBase> snapshotService = new ServiceMock<>(impl);
        snapshotService.start();

        SnapshotApiGrpc.SnapshotApiBlockingStub stub = SnapshotApiGrpc.newBlockingStub(snapshotService.channel());

        OutputSlotMock mock = new OutputSlotMock.OutputSlotMockBuilder()
            .setName("slotName")
            .setOnSuspend(() -> suspended.set(true))
            .build();

        SlotSnapshotProvider provider = slot -> new SlotSnapshot() {
            @Override
            public URI uri() {
                return URI.create("s3://some-uri.com");
            }

            @Override
            public void onChunk(ByteString chunk) {
            }

            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public void onFinish() {
                throw new RuntimeException("Bad exception");
            }
        };

        Snapshooter snapshooter = new SnapshooterImpl(IAM.Auth.newBuilder().build(), stub, provider);


        // Action
        snapshooter.registerSlot(mock, "a", "b");
        mock.chunk(ByteString.copyFromUtf8("aaa"));
        mock.chunk(ByteString.copyFromUtf8("bbb"));
        Assert.assertThrows(RuntimeException.class, () -> mock.state(Operations.SlotStatus.State.OPEN));
        mock.state(Operations.SlotStatus.State.DESTROYED);

        // Assert
        Assert.assertTrue(suspended.get());
        Assert.assertTrue(aborted.get());

        // TearDown
        snapshotService.stop();
        snapshotService.awaitTermination();
    }
}
