package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import yandex.cloud.priv.lzy.v1.LABS.ListAccessBindingsRequest;
import yandex.cloud.priv.lzy.v1.LABS.ListAccessBindingsResponse;
import yandex.cloud.priv.lzy.v1.LABS.SetAccessBindingsRequest;
import yandex.cloud.priv.lzy.v1.LABS.UpdateAccessBindingsRequest;
import yandex.cloud.priv.lzy.v1.LzyABSGrpc;

public class LzyABSService extends LzyABSGrpc.LzyABSImplBase {

    @Override
    public void listAccessBindings(ListAccessBindingsRequest request,
        StreamObserver<ListAccessBindingsResponse> responseObserver) {
        super.listAccessBindings(request, responseObserver);
    }

    @Override
    public void setAccessBindings(SetAccessBindingsRequest request,
        StreamObserver<Empty> responseObserver) {
        super.setAccessBindings(request, responseObserver);
    }

    @Override
    public void updateAccessBindings(UpdateAccessBindingsRequest request,
        StreamObserver<Empty> responseObserver) {
        super.updateAccessBindings(request, responseObserver);
    }
}
