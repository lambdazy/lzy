package ai.lzy.iam.services;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.storage.impl.DbAccessBindingClient;
import ai.lzy.iam.storage.impl.DbAccessClient;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LABS;
import ai.lzy.v1.iam.LABS.ListAccessBindingsRequest;
import ai.lzy.v1.iam.LABS.ListAccessBindingsResponse;
import ai.lzy.v1.iam.LABS.SetAccessBindingsRequest;
import ai.lzy.v1.iam.LABS.UpdateAccessBindingsRequest;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
                                   StreamObserver<ListAccessBindingsResponse> responseObserver)
    {
        if (invalidAccess(ProtoConverter.to(request.getResource()), ResourceAccessType.VIEW)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }
        Stream<IAM.AccessBinding> bindings = accessBindingClient.listAccessBindings(
                ProtoConverter.to(request.getResource())
        ).map(ProtoConverter::from);
        responseObserver.onNext(ListAccessBindingsResponse.newBuilder()
                .addAllBindings(bindings.collect(Collectors.toList())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setAccessBindings(SetAccessBindingsRequest request,
                                  StreamObserver<LABS.SetAccessBindingsResponse> responseObserver)
    {
        if (invalidAccess(ProtoConverter.to(request.getResource()), ResourceAccessType.EDIT)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }
        accessBindingClient.setAccessBindings(ProtoConverter.to(request.getResource()),
                request.getBindingsList().stream().map(ProtoConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(LABS.SetAccessBindingsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateAccessBindings(UpdateAccessBindingsRequest request,
                                     StreamObserver<LABS.UpdateAccessBindingsResponse> responseObserver)
    {
        if (invalidAccess(ProtoConverter.to(request.getResource()), ResourceAccessType.EDIT)) {
            LOG.error("Resource::{} NOT_FOUND", request.getResource());
            responseObserver.onError(Status.NOT_FOUND.asException());
            return;
        }
        accessBindingClient.updateAccessBindings(ProtoConverter.to(request.getResource()),
                request.getDeltasList().stream().map(ProtoConverter::to).collect(Collectors.toList()));
        responseObserver.onNext(LABS.UpdateAccessBindingsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private boolean invalidAccess(AuthResource resource, ResourceAccessType accessType) {
        var permission = switch (resource.type()) {
            case Workflow.TYPE -> switch (accessType) {
                case EDIT -> AuthPermission.WORKFLOW_RUN;
                case VIEW -> AuthPermission.WORKFLOW_GET;
            };
            case Whiteboard.TYPE -> switch (accessType) {
                case EDIT -> AuthPermission.WHITEBOARD_UPDATE;
                case VIEW -> AuthPermission.WHITEBOARD_GET;
            };
            default -> null;
        };
        if (permission == null) {
            return false;
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
