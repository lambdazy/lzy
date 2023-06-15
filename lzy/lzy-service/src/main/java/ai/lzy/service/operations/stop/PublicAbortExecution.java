package ai.lzy.service.operations.stop;

import ai.lzy.v1.workflow.LWFS;
import com.google.protobuf.Message;

public class PublicAbortExecution extends AbortExecution {
    public PublicAbortExecution(PublicAbortExecutionBuilder builder) {
        super(builder);
    }

    @Override
    protected Message response() {
        return LWFS.AbortWorkflowResponse.getDefaultInstance();
    }

    public static PublicAbortExecutionBuilder builder() {
        return new PublicAbortExecutionBuilder();
    }

    public static final class PublicAbortExecutionBuilder extends
        AbortExecution.AbortExecutionBuilder<PublicAbortExecutionBuilder>
    {
        @Override
        public PublicAbortExecution build() {
            return new PublicAbortExecution(this);
        }
    }
}
