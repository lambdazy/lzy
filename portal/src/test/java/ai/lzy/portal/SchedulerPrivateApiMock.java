package ai.lzy.portal;

import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.common.LME;
import ai.lzy.v1.common.LMO;
import ai.lzy.v1.scheduler.SchedulerPrivateApi;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

class SchedulerPrivateApiMock extends SchedulerPrivateGrpc.SchedulerPrivateImplBase {
    private static final Logger LOG = LogManager.getLogger(SchedulerPrivateApiMock.class);

    final Server server;
    final Map<String, WorkerHandler> workerHandlers = new ConcurrentHashMap<>();

    public SchedulerPrivateApiMock(int port) {
        this.server = GrpcUtils.newGrpcServer("localhost", port, GrpcUtils.NO_AUTH)
            .addService(this)
            .addService(new AllocatorPrivateAPIMock())
            .build();
    }

    public void start() throws IOException {
        server.start();
    }

    void stop() throws InterruptedException {
        for (var worker : workerHandlers.values()) {
            worker.shutdown();
        }
        for (var worker : workerHandlers.values()) {
            worker.awaitTermination(2, TimeUnit.SECONDS);
        }
        server.shutdown();
        server.awaitTermination();
    }

    WorkerHandler worker(String workerId) {
        return workerHandlers.get(workerId);
    }

    void startWorker(String workerId, LMO.TaskDesc taskDesc, String taskId, String executionId) {
        worker(workerId).startTask(taskDesc, taskId, executionId);
    }

    void awaitProcessing(String workerId) {
        var worker = worker(workerId);
        while (worker.active.get()) {
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    @Override
    public void registerServant(SchedulerPrivateApi.RegisterServantRequest request,
                                StreamObserver<SchedulerPrivateApi.RegisterServantResponse> responseObserver)
    {
        LOG.info("register worker: " + JsonUtils.printSingleLine(request));

        workerHandlers.put(request.getServantId(), new WorkerHandler(request));

        responseObserver.onNext(SchedulerPrivateApi.RegisterServantResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void servantProgress(SchedulerPrivateApi.ServantProgressRequest request,
                                StreamObserver<SchedulerPrivateApi.ServantProgressResponse> responseObserver)
    {
        LOG.info("worker progress: " + JsonUtils.printSingleLine(request));

        if (request.getProgress().getStatusCase() == SchedulerPrivateApi.ServantProgress.StatusCase.IDLING) {
            worker(request.getServantId()).active.set(false);
        }

        responseObserver.onNext(SchedulerPrivateApi.ServantProgressResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    static class WorkerHandler {
        private final AtomicBoolean active = new AtomicBoolean(false);

        private final WorkerApiGrpc.WorkerApiBlockingStub workerClient;
        private final ManagedChannel workerChannel;

        WorkerHandler(SchedulerPrivateApi.RegisterServantRequest req) {
            workerChannel = newGrpcChannel("localhost", req.getApiPort(), WorkerApiGrpc.SERVICE_NAME);
            workerClient = newBlockingClient(
                WorkerApiGrpc.newBlockingStub(workerChannel),
                "WorkerHandler",
                GrpcUtils.NO_AUTH_TOKEN);
        }

        public void shutdown() {
            workerClient.stop(LWS.StopRequest.getDefaultInstance());
            workerChannel.shutdown();
        }

        @SuppressWarnings("UnusedReturnValue")
        public boolean awaitTermination(long c, TimeUnit timeUnit) throws InterruptedException {
            return workerChannel.awaitTermination(c, timeUnit);
        }

        public void startTask(LMO.TaskDesc taskDesc, String taskId, String executionId) {
            if (active.compareAndSet(false, true)) {
                workerClient.configure(
                    LWS.ConfigureRequest.newBuilder().setEnv(LME.EnvSpec.getDefaultInstance()).build());
                workerClient.execute(LWS.ExecuteRequest.newBuilder()
                    .setTaskId(taskId)
                    .setExecutionId(executionId)
                    .setTaskDesc(taskDesc)
                    .build());
            } else {
                LOG.error("Worker is already active and processing some other task");
                throw new RuntimeException("Cannot start worker");
            }
        }
    }
}