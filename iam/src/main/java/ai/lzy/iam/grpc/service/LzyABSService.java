package ai.lzy.iam.grpc.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.storage.impl.DbAccessBindingClient;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.storage.impl.DbAccessClient;
import ai.lzy.iam.utils.GrpcConverter;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LABS;
import ai.lzy.v1.iam.LABS.ListAccessBindingsRequest;
import ai.lzy.v1.iam.LABS.ListAccessBindingsResponse;
import ai.lzy.v1.iam.LABS.SetAccessBindingsRequest;
import ai.lzy.v1.iam.LABS.UpdateAccessBindingsRequest;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class LzyABSService extends LzyAccessBindingServiceGrpc.LzyAccessBindingServiceImplBase {
    public static final Logger LOG = LogManager.getLogger(LzyABSService.class);

    @Inject
    private DbAccessBindingClient accessBindingClient;
    @Inject
    private DbAccessClient accessClient;

    @Override
    public void listAccessBindings(ListAccessBindingsRequest request,
                                   StreamObserver<ListAccessBindingsResponse> responseObserver) {
        if (invalidAccess(GrpcConverter.to(request.getResource()), ResourceAccessType.VIEW)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
        }
        Stream<IAM.AccessBinding> bindings = accessBindingClient.listAccessBindings(
                GrpcConverter.to(request.getResource())
        ).map(GrpcConverter::from);
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
                permission = switch (accessType) {
                    case EDIT -> AuthPermission.WORKFLOW_RUN;
                    case VIEW -> AuthPermission.WORKFLOW_GET;
                };
                break;
            case Whiteboard.TYPE:
                permission = switch (accessType) {
                    case EDIT -> AuthPermission.WHITEBOARD_UPDATE;
                    case VIEW -> AuthPermission.WHITEBOARD_GET;
                };
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
