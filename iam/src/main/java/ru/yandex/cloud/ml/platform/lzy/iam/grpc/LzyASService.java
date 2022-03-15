package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import yandex.cloud.priv.lzy.v1.IAM.Subject;
import yandex.cloud.priv.lzy.v1.LAS.AuthorizeRequest;
import yandex.cloud.priv.lzy.v1.LzyASGrpc;

@Singleton
@Requires(beans = AccessClient.class)
public class LzyASService extends LzyASGrpc.LzyASImplBase {
    public static final Logger LOG = LogManager.getLogger(LzyASService.class);

    @Inject
    AccessClient accessClient;

    @Override
    public void authorize(AuthorizeRequest request, StreamObserver<Subject> responseObserver) {
        LOG.info("Authorize user:: " + request.getSubjectId() + " to resource:: " + request.getResource().getId());
        super.authorize(request, responseObserver);
    }
}
