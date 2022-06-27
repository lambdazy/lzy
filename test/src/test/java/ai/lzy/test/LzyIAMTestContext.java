package ai.lzy.test;

import ai.lzy.priv.v1.LzyAccessServiceGrpc;
import ai.lzy.priv.v1.LzySubjectServiceGrpc;

public interface LzyIAMTestContext {
    String address();

    LzyAccessServiceGrpc.LzyAccessServiceBlockingStub accessServiceClient();

    LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectServiceClient();

    void init();

    void close();
}
