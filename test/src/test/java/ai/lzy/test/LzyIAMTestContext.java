package ai.lzy.test;

import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzyAccessServiceGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import com.google.common.net.HostAndPort;

@SuppressWarnings("UnstableApiUsage")
public interface LzyIAMTestContext {
    HostAndPort address();
    void init();
    void close();

    LzyAccessServiceGrpc.LzyAccessServiceBlockingStub accessServiceClient();

    LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectServiceClient();

    LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub accessBindingServiceClient();

    LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub authenticateServiceClient();
}
