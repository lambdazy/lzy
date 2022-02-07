package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import io.grpc.stub.StreamObserver;
import yandex.cloud.priv.lzy.v1.IAM.Subject;
import yandex.cloud.priv.lzy.v1.LAS.AuthorizeRequest;
import yandex.cloud.priv.lzy.v1.LzyASGrpc;

public class LzyASService extends LzyASGrpc.LzyASImplBase {

    @Override
    public void authorize(AuthorizeRequest request, StreamObserver<Subject> responseObserver) {
        super.authorize(request, responseObserver);
    }
}
