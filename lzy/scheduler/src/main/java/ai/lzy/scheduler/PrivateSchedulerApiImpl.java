package ai.lzy.scheduler;

import ai.lzy.model.ReturnCodes;
import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.scheduler.db.WorkerDao;
import ai.lzy.scheduler.worker.Worker;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.scheduler.SchedulerPrivateApi;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PrivateSchedulerApiImpl extends SchedulerPrivateGrpc.SchedulerPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(PrivateSchedulerApiImpl.class);
    private final WorkerDao dao;

    @Inject
    public PrivateSchedulerApiImpl(WorkerDao dao) {
        this.dao = dao;
    }

    @Override
    public void workerProgress(SchedulerPrivateApi.WorkerProgressRequest request,
                               StreamObserver<SchedulerPrivateApi.WorkerProgressResponse> responseObserver)
    {
        final Worker worker;
        try {
            worker = dao.get(request.getWorkflowName(), request.getWorkerId());
        } catch (DaoException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Database exception").asException());
            return;
        }
        if (worker == null) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Worker not found").asException());
            return;
        }
        switch (request.getProgress().getStatusCase()) {
            case EXECUTING -> worker.executingHeartbeat();
            case IDLING -> worker.idleHeartbeat();
            case CONFIGURED -> {
                if (request.getProgress().getConfigured().hasErr()) {
                    worker.notifyConfigured(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(),
                            request.getProgress().getConfigured().getErr().getDescription());
                } else {
                    worker.notifyConfigured(0, "Ok");
                }
            }

            case COMMUNICATIONCOMPLETED -> worker.notifyCommunicationCompleted();
            case FINISHED -> worker.notifyStopped(0, "Ok");
            case EXECUTIONCOMPLETED -> worker
                    .notifyExecutionCompleted(request.getProgress().getExecutionCompleted().getRc(),
                            request.getProgress().getExecutionCompleted().getDescription());
            default -> {
                LOG.error("Unknown progress from worker: {}", JsonUtils.printRequest(request));
                responseObserver.onError(Status.UNIMPLEMENTED.asException());
                return;
            }
        }
        responseObserver.onNext(SchedulerPrivateApi.WorkerProgressResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
