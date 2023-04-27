package ai.lzy.channelmanager;

import ai.lzy.longrunning.Operation;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

public class SlotServiceMock {

    private static final Duration OPERATION_DURATION = Duration.of(500, ChronoUnit.MILLIS);

    private final ConcurrentHashMap<String, Operation> operations = new ConcurrentHashMap<>();
    private final OperationServiceMock opService = new OperationServiceMock();
    private final SlotsApiMock slotApiService = new SlotsApiMock();

    public OperationServiceMock operationService() {
        return opService;
    }

    public SlotsApiMock slotApiService() {
        return slotApiService;
    }

    public class OperationServiceMock extends LongRunningServiceGrpc.LongRunningServiceImplBase {

        @Override
        public void get(LongRunning.GetOperationRequest request,
                        StreamObserver<LongRunning.Operation> responseObserver)
        {
            Operation op = operations.get(request.getOperationId());
            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
        }
    }

    public class SlotsApiMock extends LzySlotsApiGrpc.LzySlotsApiImplBase {

        @Override
        public void connectSlot(LSA.ConnectSlotRequest request,
                                StreamObserver<LongRunning.Operation> responseObserver)
        {
            Operation op = Operation.create(this.getClass().getSimpleName(),
                "connectSlot from " + request.getFrom() + " to " + request.getTo(), null, null);
            operations.put(op.id(), op);
            responseObserver.onNext(op.toProto());
            responseObserver.onCompleted();
            LockSupport.parkNanos(OPERATION_DURATION.toNanos());
            op.completeWith(Any.pack(LSA.ConnectSlotResponse.getDefaultInstance()));
            operations.put(op.id(), op);
        }

        @Override
        public void disconnectSlot(LSA.DisconnectSlotRequest request,
                                   StreamObserver<LSA.DisconnectSlotResponse> responseObserver)
        {
            responseObserver.onNext(LSA.DisconnectSlotResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }

        @Override
        public void destroySlot(LSA.DestroySlotRequest request,
                                StreamObserver<LSA.DestroySlotResponse> responseObserver)
        {
            responseObserver.onNext(LSA.DestroySlotResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
