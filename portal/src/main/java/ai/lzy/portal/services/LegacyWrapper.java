package ai.lzy.portal.services;

import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.v1.deprecated.LzyFsApi;
import ai.lzy.v1.deprecated.LzyFsApi.*;
import ai.lzy.v1.deprecated.LzyFsGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.slots.LSA;
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

    private final PortalSlotsService slotsApi;

    private final LocalOperationService operationService;

    public LegacyWrapper(@Named("PortalSlotsService") PortalSlotsService slotsApi,
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

        if (errRef[0] != null) {
            LegacyWrapper.this.onError(errRef[0], resp);
            return;
        }

        if (!operationService.awaitOperationCompletion(opRef[0].getId(), Duration.ofSeconds(5))) {
            LOG.error("[{}] Cannot await operation completion: { opId: {} }", slotsApi.getPortalId(), opRef[0].getId());
            resp.onError(Status.INTERNAL.withDescription("Cannot connect slot").asRuntimeException());
            return;
        }

        var op = operationService.get(opRef[0].getId());

        assert op != null;

        if (op.error() != null) {
            LegacyWrapper.this.onError(op.error().asRuntimeException(), resp);
        } else {
            resp.onNext(LzyFsApi.SlotCommandStatus.getDefaultInstance());
            resp.onCompleted();
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
