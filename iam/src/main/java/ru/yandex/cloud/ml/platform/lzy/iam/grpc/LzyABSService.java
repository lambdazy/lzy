package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessBindingClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Workflow;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.lzy.v1.LABS;
import yandex.cloud.lzy.v1.LABS.ListAccessBindingsRequest;
import yandex.cloud.lzy.v1.LABS.ListAccessBindingsResponse;
import yandex.cloud.lzy.v1.LABS.SetAccessBindingsRequest;
import yandex.cloud.lzy.v1.LABS.UpdateAccessBindingsRequest;
import yandex.cloud.lzy.v1.LzyAccessBindingServiceGrpc;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LzyABSService extends LzyAccessBindingServiceGrpc.LzyAccessBindingServiceImplBase {
    public static final Logger LOG = LogManager.getLogger(LzyABSService.class);

    @Inject
    private AccessBindingClient accessBindingClient;
    @Inject
    private AccessClient accessClient;

    @Override
    public void listAccessBindings(ListAccessBindingsRequest request,
                                   StreamObserver<ListAccessBindingsResponse> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), ResourceAccessType.VIEW)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        Stream<IAM.AccessBinding> bindings = accessBindingClient.listAccessBindings(GrpcConverter.to(request.getResource()))
                .map(GrpcConverter::from);
        responseObserver.onNext(ListAccessBindingsResponse.newBuilder()
                .addAllBindings(bindings.collect(Collectors.toList())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setAccessBindings(SetAccessBindingsRequest request,
                                  StreamObserver<LABS.SetAccessBindingsResponse> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), ResourceAccessType.EDIT)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        accessBindingClient.setAccessBindings(GrpcConverter.to(request.getResource()),
                request.getBindingsList().stream().map(GrpcConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(LABS.SetAccessBindingsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateAccessBindings(UpdateAccessBindingsRequest request,
                                     StreamObserver<LABS.UpdateAccessBindingsResponse> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), ResourceAccessType.EDIT)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        accessBindingClient.updateAccessBindings(GrpcConverter.to(request.getResource()),
                request.getDeltasList().stream().map(GrpcConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(LABS.UpdateAccessBindingsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private boolean invalidAccess(AuthResource resource, ResourceAccessType accessType) {
        AuthPermission permission;
        switch (resource.type()) {
            case Workflow.TYPE:
                switch (accessType) {
                    case EDIT:
                        permission = AuthPermission.WORKFLOW_RUN;
                        break;
                    case VIEW:
                        permission = AuthPermission.WORKFLOW_GET;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + accessType);
                }
                break;
            case Whiteboard.TYPE:
                switch (accessType) {
                    case EDIT:
                        permission = AuthPermission.WHITEBOARD_UPDATE;
                        break;
                    case VIEW:
                        permission = AuthPermission.WHITEBOARD_GET;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + accessType);
                }
                break;
            default:
                return true;
        }
        return !accessClient.hasResourcePermission(
                Objects.requireNonNull(AuthenticationContext.current()).getSubject(),
                resource.resourceId(),
                permission);
    }

    private enum ResourceAccessType {
        VIEW,
        EDIT,
    }
}
