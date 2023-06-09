package ai.lzy.service.operations.stop;

import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;

import java.util.List;
import java.util.function.Supplier;

public abstract class AbortExecution extends StopExecution {
    private final GraphExecutorBlockingStub graphClient;

    private final List<Supplier<StepResult>> steps;

    protected AbortExecution(AbortExecutionBuilder<?> builder) {
        super(builder);
        this.graphClient = builder.graphClient;
        this.steps = List.of(stopGraphs(), finishPortal(), waitFinishPortal(), freePortalVm(), deleteAllocSession(),
            waitDeleteAllocSession(), deletePortalSubject(), destroyChannels(), deleteKafkaTopic(), this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
    }

    private Supplier<StepResult> stopGraphs() {
        return new StopGraphs(stepCtx(), state(), graphClient);
    }

    private Supplier<StepResult> finishPortal() {
        return new FinishPortal(stepCtx(), state());
    }

    private Supplier<StepResult> waitFinishPortal() {
        return new WaitFinishPortal(stepCtx(), state());
    }

    private Supplier<StepResult> freePortalVm() {
        return new FreePortalVm(stepCtx(), state(), allocClient());
    }

    private Supplier<StepResult> deleteAllocSession() {
        return new DeleteAllocatorSession(stepCtx(), state(), allocClient());
    }

    private Supplier<StepResult> waitDeleteAllocSession() {
        return new WaitDeleteAllocatorSession(stepCtx(), state(), allocOpClient());
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

    public abstract static class AbortExecutionBuilder<T extends AbortExecutionBuilder<T>>
        extends StopExecutionBuilder<T>
    {
        private GraphExecutorBlockingStub graphClient;

        public T setGraphClient(GraphExecutorBlockingStub graphClient) {
            this.graphClient = graphClient;
            return self();
        }
    }
}
