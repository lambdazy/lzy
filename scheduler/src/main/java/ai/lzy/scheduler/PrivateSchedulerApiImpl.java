package ai.lzy.scheduler;

import ai.lzy.model.JsonUtils;
import ai.lzy.model.ReturnCodes;
import ai.lzy.priv.v2.lzy.SchedulerPrivateApi;
import ai.lzy.priv.v2.lzy.SchedulerPrivateGrpc;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.grpc.RemoteAddressContext;
import ai.lzy.scheduler.servant.Servant;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.curator.shaded.com.google.common.net.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PrivateSchedulerApiImpl extends SchedulerPrivateGrpc.SchedulerPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(PrivateSchedulerApiImpl.class);
    private final ServantDao dao;

    @Inject
    public PrivateSchedulerApiImpl(ServantDao dao) {
        this.dao = dao;
    }

    @Override
    public void servantProgress(SchedulerPrivateApi.ServantProgressRequest request,
                                StreamObserver<SchedulerPrivateApi.ServantProgressResponse> responseObserver) {
        final Servant servant;
        try {
            servant = dao.get(request.getWorkflowName(), request.getServantId());
        } catch (DaoException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Database exception").asException());
            return;
        }
        if (servant == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Servant not found").asException());
            return;
        }
        switch (request.getProgress().getStatusCase()) {
            case EXECUTING -> servant.executingHeartbeat();
            case IDLING -> servant.idleHeartbeat();
            case CONFIGURED -> {
                if (request.getProgress().getConfigured().hasErr()) {
                    servant.notifyConfigured(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(),
                            request.getProgress().getConfigured().getErr().getDescription());
                } else {
                    servant.notifyConfigured(0, "Ok");
                }
            }

            case COMMUNICATIONCOMPLETED -> servant.notifyCommunicationCompleted();
            case FINISHED -> servant.notifyStopped(0, "Ok");
            case EXECUTIONCOMPLETED -> servant
                    .notifyExecutionCompleted(request.getProgress().getExecutionCompleted().getRc(),
                            request.getProgress().getExecutionCompleted().getDescription());
            default -> {
                LOG.error("Unknown progress from servant: {}", JsonUtils.printRequest(request));
                responseObserver.onError(Status.UNIMPLEMENTED.asException());
                return;
            }
        }
        responseObserver.onNext(SchedulerPrivateApi.ServantProgressResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void registerServant(SchedulerPrivateApi.RegisterServantRequest request,
                                StreamObserver<SchedulerPrivateApi.RegisterServantResponse> responseObserver) {
        RemoteAddressContext context = RemoteAddressContext.KEY.get();
        final Servant servant;
        try {
            servant = dao.get(request.getWorkflowName(), request.getServantId());
        } catch (DaoException e) {
            responseObserver.onError(Status.INTERNAL.asException());
            return;
        }

        if (servant == null) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription("Servant not found in workflow").asException());
            return;
        }
        var host = context.remoteHost();
        if (host == null) {
            responseObserver.onError(Status.INTERNAL.withDescription("Cannot get remote peer host").asException());
            return;
        }
        servant.notifyConnected(HostAndPort.fromParts(host, request.getApiPort()));
        responseObserver.onNext(SchedulerPrivateApi.RegisterServantResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
