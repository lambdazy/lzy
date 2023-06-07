package ai.lzy.service.operations.stop;

import ai.lzy.v1.workflow.LWFPS;
import com.google.protobuf.Message;

public final class PrivateAbortExecution extends AbortExecution {
    public PrivateAbortExecution(PrivateAbortExecutionBuilder builder) {
        super(builder);
    }

    @Override
    protected Message response() {
        return LWFPS.AbortExecutionResponse.getDefaultInstance();
    }

    public static PrivateAbortExecutionBuilder builder() {
        return new PrivateAbortExecutionBuilder();
    }

    public static final class PrivateAbortExecutionBuilder extends AbortExecutionBuilder<PrivateAbortExecutionBuilder> {
        @Override
        public PrivateAbortExecution build() {
            return new PrivateAbortExecution(this);
        }
    }
}
