package ai.lzy.service.operations.stop;

import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

public final class FinishExecution extends StopExecution {
    private final List<Supplier<StepResult>> steps;

    private FinishExecution(FinishExecutionBuilder builder) {
        super(builder);
        this.steps = List.of(destroyChannels(), deleteKafkaTopic(), scheduleAllocSessionRemoval(), this::complete);
    }

    @Override
    protected List<Supplier<StepResult>> steps() {
        return steps;
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
        return LWFS.FinishWorkflowResponse.getDefaultInstance();
    }

    public static FinishExecutionBuilder builder() {
        return new FinishExecutionBuilder();
    }

    public static final class FinishExecutionBuilder extends StopExecutionBuilder<FinishExecutionBuilder> {

        @Override
        public FinishExecution build() {
            return new FinishExecution(this);
        }
    }
}
