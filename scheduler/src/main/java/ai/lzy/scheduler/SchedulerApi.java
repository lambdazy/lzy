package ai.lzy.scheduler;

import ai.lzy.model.JsonUtils;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.logs.GrpcLogsInterceptor;
import ai.lzy.priv.v2.SchedulerApi.*;
import ai.lzy.priv.v2.SchedulerGrpc;
import ai.lzy.priv.v2.lzy.SchedulerPrivateApi.RegisterServantRequest;
import ai.lzy.priv.v2.lzy.SchedulerPrivateApi.RegisterServantResponse;
import ai.lzy.priv.v2.lzy.SchedulerPrivateApi.ServantProgressRequest;
import ai.lzy.priv.v2.lzy.SchedulerPrivateApi.ServantProgressResponse;
import ai.lzy.priv.v2.lzy.SchedulerPrivateGrpc.SchedulerPrivateImplBase;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.DaoException;
import ai.lzy.scheduler.db.ServantDao;
import ai.lzy.scheduler.grpc.RemoteAddressContext;
import ai.lzy.scheduler.grpc.RemoteAddressInterceptor;
import ai.lzy.scheduler.models.TaskDesc;
import ai.lzy.scheduler.servant.Scheduler;
import ai.lzy.scheduler.servant.Servant;
import ai.lzy.scheduler.task.Task;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SchedulerApi {
    private static final Logger LOG = LogManager.getLogger(SchedulerApi.class);
    private final Server server;
    private final Impl impl;

    @Singleton
    private static class Impl extends SchedulerGrpc.SchedulerImplBase {
        private final Scheduler scheduler;

        @Inject
        private Impl(Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void schedule(TaskScheduleRequest request, StreamObserver<TaskScheduleResponse> responseObserver) {
            final Task task;
            try {
                task = scheduler.execute(request.getWorkflowId(), request.getWorkflowName(),
                    TaskDesc.fromGrpc(request.getTask()));
            } catch (StatusException e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(TaskScheduleResponse.newBuilder()
                .setStatus(buildTaskStatus(task))
                .build());
            responseObserver.onCompleted();
        }

        @Override
        public void status(TaskStatusRequest request, StreamObserver<TaskStatusResponse> responseObserver) {
            final Task task;
            try {
                task = scheduler.status(request.getWorkflowId(), request.getTaskId());
            } catch (StatusException e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(TaskStatusResponse.newBuilder()
                .setStatus(buildTaskStatus(task))
                .buildPartial());
            responseObserver.onCompleted();
        }

        @Override
        public void list(TaskListRequest request, StreamObserver<TaskListResponse> responseObserver) {
            final List<Task> tasks;
            try {
                tasks = scheduler.list(request.getWorkflowId());
            } catch (StatusException e) {
                responseObserver.onError(e);
                return;
            }
            List<TaskStatus> statuses = tasks.stream()
                .map(Impl::buildTaskStatus)
                .toList();
            responseObserver.onNext(TaskListResponse.newBuilder().addAllStatus(statuses).build());
            responseObserver.onCompleted();
        }

        @Override
        public void stop(TaskStopRequest request, StreamObserver<TaskStopResponse> responseObserver) {
            final Task task;
            try {
                task = scheduler.stopTask(request.getWorkflowId(), request.getTaskId(), request.getIssue());
            } catch (StatusException e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(TaskStopResponse.newBuilder().setStatus(buildTaskStatus(task)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void killAll(KillAllRequest request, StreamObserver<KillAllResponse> responseObserver) {
            try {
                scheduler.killAll(request.getWorkflowName(), request.getIssue());
            } catch (StatusException e) {
                responseObserver.onError(e);
                return;
            }
            responseObserver.onNext(KillAllResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        private static TaskStatus buildTaskStatus(Task task) {
            var builder = TaskStatus.newBuilder()
                    .setTaskId(task.taskId())
                    .setWorkflowId(task.workflowId())
                    .setZygoteName(task.description().zygote().name());

            Integer rc = task.rc();
            int rcInt = rc == null ? 0 : rc;
            var b = switch (task.status()) {
                case QUEUE -> builder.setQueue(TaskStatus.Queue.newBuilder().build());
                case SCHEDULED, EXECUTING -> builder.setExecuting(TaskStatus.Executing.newBuilder().build());
                case ERROR -> builder.setError(TaskStatus.Error.newBuilder()
                        .setDescription(task.errorDescription())
                        .setRc(rcInt)
                        .build());
                case SUCCESS -> builder.setSuccess(TaskStatus.Success.newBuilder()
                        .setRc(rcInt)
                        .build());
            };
            return b.build();
        }

        public void close() {
            scheduler.terminate();
        }

        public void awaitTermination() throws InterruptedException {
            scheduler.awaitTermination();
        }
    }

    @Singleton
    private static class PrivateApiImpl extends SchedulerPrivateImplBase {
        private final ServantDao dao;
        private final ServantsAllocator allocator;

        @Inject
        private PrivateApiImpl(ServantDao dao, ServantMetaStorage meta, ServantsAllocator allocator) {
            this.dao = dao;
            this.allocator = allocator;
        }

        @Override
        public void servantProgress(ServantProgressRequest request,
                                    StreamObserver<ServantProgressResponse> responseObserver) {
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
            responseObserver.onNext(ServantProgressResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void registerServant(RegisterServantRequest request,
                                    StreamObserver<RegisterServantResponse> responseObserver) {
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
            servant.notifyConnected(context.address());
            responseObserver.onNext(RegisterServantResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    public SchedulerApi() {
        try (final ApplicationContext context = ApplicationContext.run()) {
            impl = context.getBean(Impl.class);
            PrivateApiImpl privateApi = context.getBean(PrivateApiImpl.class);
            final ServiceConfig config = context.getBean(ServiceConfig.class);

            ServerBuilder<?> builder = NettyServerBuilder.forPort(config.port())
                    .intercept(new RemoteAddressInterceptor())
                    .permitKeepAliveWithoutCalls(true)
                    .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES);

            builder.addService(ServerInterceptors.intercept(impl, new GrpcLogsInterceptor()));
            builder.addService(ServerInterceptors.intercept(privateApi, new GrpcLogsInterceptor()));
            server = builder.build();


            try {
                server.start();
            } catch (IOException e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        server.shutdown();
        impl.close();
    }

    public void awaitTermination() throws InterruptedException {
        impl.awaitTermination();
        server.awaitTermination();
    }

    public static void main(String[] args) {
        SchedulerApi api = new SchedulerApi();
        final Thread thread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Stopping GraphExecutor service");
            api.close();
            while (true) {
                try {
                    thread.join();
                    break;
                } catch (InterruptedException e) {
                    LOG.debug(e);
                }
            }
        }));
        while (true) {
            try {
                api.awaitTermination();
                break;
            } catch (InterruptedException ignored) {
                // ignored
            }
        }
    }

}
