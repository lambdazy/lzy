package ai.lzy.service.operations.stop;

import ai.lzy.v1.graph.GraphExecutorGrpc.GraphExecutorBlockingStub;
import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

public final class AbortExecution extends StopExecution {
    private final GraphExecutorBlockingStub graphClient;

    private final List<Supplier<StepResult>> steps;

    public AbortExecution(AbortExecutionBuilder builder) {
        super(builder);
        this.graphClient = builder.graphClient;
        this.steps = List.of(stopGraphs(), destroyChannels(), deleteKafkaTopic(), scheduleAllocSessionRemoval(),
            this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
    }

    private Supplier<StepResult> stopGraphs() {
        return new StopGraphs(stepCtx(), state(), graphClient);
    }

    private Supplier<StepResult> destroyChannels() {
        return new DestroyChannels(stepCtx(), state(), channelsClient());
    }

    private Supplier<StepResult> deleteKafkaTopic() {
        return new DeleteKafkaTopic(stepCtx(), state(), kafkaClient(), kafkaLogsListeners(), s3SinkClient());
    }

    private Supplier<StepResult> scheduleAllocSessionRemoval() {
        return new ScheduleAllocSessionRemoval(stepCtx(), state(), serviceCfg().getAllocatorVmCacheTimeout());
    }

    @Override
    protected Message response() {
        return LWFS.AbortWorkflowResponse.getDefaultInstance();
    }

    public static AbortExecutionBuilder builder() {
        return new AbortExecutionBuilder();
    }

    public static class AbortExecutionBuilder extends StopExecutionBuilder<AbortExecutionBuilder> {
        private GraphExecutorBlockingStub graphClient;

        public AbortExecutionBuilder setGraphClient(GraphExecutorBlockingStub graphClient) {
            this.graphClient = graphClient;
            return self();
        }

        @Override
        public AbortExecution build() {
            return new AbortExecution(this);
        }
    }
}
