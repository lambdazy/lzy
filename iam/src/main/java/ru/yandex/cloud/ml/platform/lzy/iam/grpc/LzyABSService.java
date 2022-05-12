package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Workflow;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.lzy.v1.LABS.ListAccessBindingsRequest;
import yandex.cloud.lzy.v1.LABS.ListAccessBindingsResponse;
import yandex.cloud.lzy.v1.LABS.SetAccessBindingsRequest;
import yandex.cloud.lzy.v1.LABS.UpdateAccessBindingsRequest;
import yandex.cloud.lzy.v1.LzyAccessBindingServiceGrpc;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LzyABSService extends LzyAccessBindingServiceGrpc.LzyAccessBindingServiceImplBase {

    @Inject
    private AccessBindingClient client;
    @Inject
    private AccessClient accessClient;

    @Override
    public void listAccessBindings(ListAccessBindingsRequest request,
                                   StreamObserver<ListAccessBindingsResponse> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), false)) {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        Stream<IAM.AccessBinding> bindings = client.listAccessBindings(GrpcConverter.to(request.getResource()))
                .map(GrpcConverter::from);
        responseObserver.onNext(ListAccessBindingsResponse.newBuilder()
                .addAllBindings(bindings.collect(Collectors.toList())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setAccessBindings(SetAccessBindingsRequest request,
                                  StreamObserver<Empty> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), true)) {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        client.setAccessBindings(GrpcConverter.to(request.getResource()),
                request.getBindingsList().stream().map(GrpcConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateAccessBindings(UpdateAccessBindingsRequest request,
                                     StreamObserver<Empty> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), true)) {
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        client.updateAccessBindings(GrpcConverter.to(request.getResource()),
                request.getDeltasList().stream().map(GrpcConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    private boolean invalidAccess(AuthResource resource, boolean edit) {
        AuthPermission permission;
        switch (resource.type()) {
            case Workflow.TYPE:
                permission = edit ? AuthPermission.WORKFLOW_RUN : AuthPermission.WORKFLOW_GET;
                break;
            case Whiteboard.TYPE:
                permission = edit ? AuthPermission.WHITEBOARD_UPDATE : AuthPermission.WHITEBOARD_GET;
                break;
            default:
                return true;
        }
        return !accessClient.hasResourcePermission(
                Objects.requireNonNull(AuthenticationContext.current()).getSubject().id(),
                resource.resourceId(),
                permission);
    }
}
