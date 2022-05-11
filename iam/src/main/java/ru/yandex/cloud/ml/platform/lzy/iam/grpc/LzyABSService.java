package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.lzy.v1.LABS.ListAccessBindingsRequest;
import yandex.cloud.lzy.v1.LABS.ListAccessBindingsResponse;
import yandex.cloud.lzy.v1.LABS.SetAccessBindingsRequest;
import yandex.cloud.lzy.v1.LABS.UpdateAccessBindingsRequest;
import yandex.cloud.lzy.v1.LzyAccessBindingServiceGrpc;

public class LzyABSService extends LzyAccessBindingServiceGrpc.LzyAccessBindingServiceImplBase {

    @Inject
    private AccessBindingClient client;

    @Override
    public void listAccessBindings(ListAccessBindingsRequest request,
        StreamObserver<ListAccessBindingsResponse> responseObserver) {
        Stream<IAM.AccessBinding> bindings = client.listAccessBindings(GrpcConverter.to(request.getResource()))
            .map(GrpcConverter::from);
        responseObserver.onNext(ListAccessBindingsResponse.newBuilder()
            .addAllBindings(bindings.collect(Collectors.toList())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setAccessBindings(SetAccessBindingsRequest request,
        StreamObserver<Empty> responseObserver) {
        client.setAccessBindings(GrpcConverter.to(request.getResource()),
            request.getBindingsList().stream().map(GrpcConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateAccessBindings(UpdateAccessBindingsRequest request,
        StreamObserver<Empty> responseObserver) {
        client.updateAccessBindings(GrpcConverter.to(request.getResource()),
            request.getDeltasList().stream().map(GrpcConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
