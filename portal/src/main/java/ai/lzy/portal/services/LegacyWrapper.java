package ai.lzy.portal.services;

import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsApi.*;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.slots.LSA;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

@Singleton
public class LegacyWrapper extends LzyFsGrpc.LzyFsImplBase {
    private static final Logger LOG = LogManager.getLogger(LegacyWrapper.class);

    private final LzySlotsApiGrpc.LzySlotsApiImplBase slotsApi;

    private final LocalOperationService operationService;

    public LegacyWrapper(@Named("PortalSlotsService") LzySlotsApiGrpc.LzySlotsApiImplBase slotsApi,
                         @Named("PortalOperationsService") LocalOperationService operationService)
    {
        this.slotsApi = slotsApi;
        this.operationService = operationService;
    }

    @Override
    public void createSlot(LzyFsApi.CreateSlotRequest request, StreamObserver<LzyFsApi.SlotCommandStatus> resp) {
        resp.onError(Status.UNIMPLEMENTED.withDescription("Not supported in portal").asException());
    }

    @Override
    public void connectSlot(ConnectSlotRequest request, StreamObserver<SlotCommandStatus> resp) {
        final LongRunning.Operation[] opRef = {null};
        final Throwable[] errRef = {null};

        slotsApi.connectSlot(
            LSA.ConnectSlotRequest.newBuilder()
                .setFrom(request.getFrom())
                .setTo(request.getTo())
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LongRunning.Operation value) {
                    opRef[0] = value;
                }

                @Override
                public void onError(Throwable t) {
                    errRef[0] = t;
                }

                @Override
                public void onCompleted() {
                    System.out.println("1");
                }
            }
        );

        var internalErrorMessage = "Cannot connect slot";
        var opId = opRef[0].getId();
        var opSnapshot = operationService.awaitOperationCompletion(opId, Duration.ofMillis(50), Duration.ofSeconds(5));

        if (opSnapshot == null) {
            LOG.error("Can not find operation with id: { opId: {} }", opId);
            resp.onError(Status.INTERNAL.asRuntimeException());
            return;
        }

        if (opSnapshot.done()) {
            if (opSnapshot.response() != null) {
                resp.onNext(LzyFsApi.SlotCommandStatus.getDefaultInstance());
                resp.onCompleted();
            } else {
                var error = errRef[0];
                assert error != null;
                LegacyWrapper.this.onError(error, resp);
            }
        } else {
            LOG.error("Waiting deadline exceeded, operation: {}", opSnapshot.toShortString());
            resp.onError(Status.INTERNAL.withDescription(internalErrorMessage).asRuntimeException());
        }
    }

    @Override
    public void disconnectSlot(DisconnectSlotRequest request, StreamObserver<SlotCommandStatus> response) {
        slotsApi.disconnectSlot(
            LSA.DisconnectSlotRequest.newBuilder()
                .setSlotInstance(request.getSlotInstance())
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LSA.DisconnectSlotResponse value) {
                    response.onNext(SlotCommandStatus.getDefaultInstance());
                }

                @Override
                public void onError(Throwable t) {
                    LegacyWrapper.this.onError(t, response);
                }

                @Override
                public void onCompleted() {
                    response.onCompleted();
                }
            }
        );
    }

    @Override
    public void statusSlot(LzyFsApi.StatusSlotRequest request, StreamObserver<SlotCommandStatus> resp) {
        slotsApi.statusSlot(
            LSA.StatusSlotRequest.newBuilder()
                .setSlotInstance(request.getSlotInstance())
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LSA.StatusSlotResponse value) {
                    resp.onNext(SlotCommandStatus.newBuilder()
                        .setStatus(value.getStatus())
                        .build());
                }

                @Override
                public void onError(Throwable t) {
                    LegacyWrapper.this.onError(t, resp);
                }

                @Override
                public void onCompleted() {
                    resp.onCompleted();
                }
            }
        );
    }

    @Override
    public void destroySlot(DestroySlotRequest request, StreamObserver<SlotCommandStatus> resp) {
        slotsApi.destroySlot(
            LSA.DestroySlotRequest.newBuilder()
                .setSlotInstance(request.getSlotInstance())
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LSA.DestroySlotResponse value) {
                    resp.onNext(SlotCommandStatus.getDefaultInstance());
                }

                @Override
                public void onError(Throwable t) {
                    LegacyWrapper.this.onError(t, resp);
                }

                @Override
                public void onCompleted() {
                    resp.onCompleted();
                }
            }
        );
    }

    @Override
    public void openOutputSlot(SlotRequest request, StreamObserver<LzyFsApi.Message> response) {
        response.onError(Status.UNIMPLEMENTED.withDescription("Legacy API").asException());
    }

    private void onError(Throwable t, StreamObserver<SlotCommandStatus> response) {
        if (t instanceof StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND ||
                e.getStatus().getCode() == Status.Code.ALREADY_EXISTS)
            {
                response.onNext(SlotCommandStatus.newBuilder()
                    .setRc(SlotCommandStatus.RC.newBuilder()
                        .setCode(SlotCommandStatus.RC.Code.ERROR)
                        .setDescription(e.getStatus().getDescription())
                        .build())
                    .build());
                response.onCompleted();
                return;
            }
        }
        response.onError(t);
    }
}
