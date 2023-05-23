package ai.lzy.service.operations.stop;

import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

public final class AbortExecution extends StopExecution {
    private final GraphExecutorBlockingStub graphClient;

    private AbortExecution(AbortExecutionBuilder builder) {
        super(builder);
        this.graphClient = builder.graphClient;
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return List.of(stopGraphs(), finishPortal(), waitFinishPortal(), freePortalVm(),
            deleteAllocSession(), deletePortalSubject(), destroyChannels(), deleteKafkaTopic(), this::complete);
    }

    private Supplier<StepResult> stopGraphs() {
        return new StopGraphs(stepCtx(), state(), graphClient);
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

    private Supplier<StepResult> deletePortalSubject() {
        return new DeletePortalSubject(stepCtx(), state(), subjClient());
    }

    private Supplier<StepResult> destroyChannels() {
        return new DestroyChannels(stepCtx(), state(), channelsClient());
    }

    private Supplier<StepResult> deleteKafkaTopic() {
        return new DeleteKafkaTopic(stepCtx(), state(), kafkaClient(), kafkaLogsListeners(), s3SinkClient());
    }

    @Override
    protected Message response() {
        return LWFS.AbortWorkflowResponse.getDefaultInstance();
    }

    public static AbortExecutionBuilder builder() {
        return new AbortExecutionBuilder();
    }

    public static final class AbortExecutionBuilder extends StopExecutionBuilder<AbortExecutionBuilder> {
        private GraphExecutorBlockingStub graphClient;

        public AbortExecutionBuilder setGraphClient(GraphExecutorBlockingStub graphClient) {
            this.graphClient = graphClient;
            return this;
        }

        @Override
        public AbortExecution build() {
            return new AbortExecution(this);
        }
    }
}
