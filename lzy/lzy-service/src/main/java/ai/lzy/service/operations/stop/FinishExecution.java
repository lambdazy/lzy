package ai.lzy.service.operations.stop;

import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

public final class FinishExecution extends StopExecution {
    private final LongRunningServiceBlockingStub allocOpClient;
    private final LongRunningServiceBlockingStub channelsOpService;

    private final List<Supplier<StepResult>> steps;

    private FinishExecution(FinishExecutionBuilder builder) {
        super(builder);
        this.allocOpClient = builder.allocOpClient;
        this.channelsOpService = builder.channelsOpClient;
        this.steps = List.of(finishPortal(), waitFinishPortal(), freePortalVm(), deleteAllocSession(),
            waitDeleteAllocSession(), deletePortalSubject(), destroyChannels(),
            waitDestroyChannels(), deleteKafkaTopic(), this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
    }

    private Supplier<StepResult> finishPortal() {
        return new FinishPortal(stepCtx(), state(), portalClient());
    }

    private Supplier<StepResult> waitFinishPortal() {
        return new WaitFinishPortal(stepCtx(), state(), portalOpClient());
    }

    private Supplier<StepResult> freePortalVm() {
        return new FreePortalVm(stepCtx(), state(), allocClient());
    }

    private Supplier<StepResult> deleteAllocSession() {
        return new DeleteAllocatorSession(stepCtx(), state(), allocClient());
    }

    private Supplier<StepResult> waitDeleteAllocSession() {
        return new WaitDeleteAllocatorSession(stepCtx(), state(), allocOpClient);
    }

    private Supplier<StepResult> deletePortalSubject() {
        return new DeletePortalSubject(stepCtx(), state(), subjClient());
    }

    private Supplier<StepResult> destroyChannels() {
        return new DestroyChannels(stepCtx(), state(), channelsClient());
    }

    private Supplier<StepResult> waitDestroyChannels() {
        return new WaitDestroyChannels(stepCtx(), state(), channelsOpService);
    }

    private Supplier<StepResult> deleteKafkaTopic() {
        return new DeleteKafkaTopic(stepCtx(), state(), kafkaClient(), kafkaLogsListeners(), s3SinkClient());
    }

    @Override
    protected Message response() {
        return LWFS.FinishWorkflowResponse.getDefaultInstance();
    }

    public static FinishExecutionBuilder builder() {
        return new FinishExecutionBuilder();
    }

    public static final class FinishExecutionBuilder extends StopExecutionBuilder<FinishExecutionBuilder> {
        private LongRunningServiceBlockingStub allocOpClient;
        private LongRunningServiceBlockingStub channelsOpClient;

        public FinishExecutionBuilder setAllocOpClient(LongRunningServiceBlockingStub allocOpClient) {
            this.allocOpClient = allocOpClient;
            return this;
        }

        public FinishExecutionBuilder setChannelsOpClient(LongRunningServiceBlockingStub channelsOpClient) {
            this.channelsOpClient = channelsOpClient;
            return this;
        }

        @Override
        public FinishExecution build() {
            return new FinishExecution(this);
        }
    }
}
