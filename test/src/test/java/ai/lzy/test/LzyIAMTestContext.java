package ai.lzy.test;

import ai.lzy.priv.v1.LzyAccessServiceGrpc;
import ai.lzy.priv.v1.LzySubjectServiceGrpc;
import ai.lzy.v1.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;

@SuppressWarnings("UnstableApiUsage")
public interface LzyIAMTestContext {
    HostAndPort address();

    LzyAccessServiceGrpc.LzyAccessServiceBlockingStub accessServiceClient();

    LzySubjectServiceGrpc.LzySubjectServiceBlockingStub subjectServiceClient();

    LzyAccessBindingServiceGrpc.LzyAccessBindingServiceBlockingStub accessBindingServiceClient();

    LzyAuthenticateServiceGrpc.LzyAuthenticateServiceBlockingStub authenticateServiceClient();

    void init();

    void close();
}
