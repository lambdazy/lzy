package ru.yandex.cloud.ml.platform.lzy.server;

import io.grpc.*;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.exceptions.NoSuchBeanException;
import jakarta.inject.Inject;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.*;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ru.yandex.cloud.ml.platform.lzy.server.configs.StorageConfigs;
import ru.yandex.cloud.ml.platform.lzy.server.local.ServantEndpoint;
import ru.yandex.cloud.ml.platform.lzy.server.mem.ZygoteRepositoryImpl;
import ru.yandex.cloud.ml.platform.lzy.server.storage.StorageCredentialsProvider;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsRequest;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetSessionsResponse.Builder;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.SessionDescription;

import static ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter.to;
import static ru.yandex.cloud.ml.platform.lzy.server.task.Task.State.DESTROYED;
import static ru.yandex.cloud.ml.platform.lzy.server.task.Task.State.FINISHED;

public class LzyServer {

    private static final Logger LOG;

    static{
        // This is to avoid this bug: https://issues.apache.org/jira/browse/LOG4J2-2375
        // KafkaLogsConfiguration will fall, so then we must call reconfigure
        ProducerConfig.configNames();
        LoggerContext ctx = (LoggerContext)LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyServer.class);
    }


    private static final Options options = new Options();
    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
    }

    private static final String LZY_SERVER_HOST_ENV = "LZY_SERVER_HOST";
    private static final String DEFAULT_LZY_SERVER_LOCALHOST = "http://localhost";

    public static void main(String[] args) throws IOException, InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        CommandLine parse = null;
        try {
            parse = cliParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("lzy-server", options);
            System.exit(-1);
        }
        final int port = Integer.parseInt(parse.getOptionValue('p', "8888"));
        final String lzyServerHost;
        if (System.getenv().containsKey(LZY_SERVER_HOST_ENV)) {
            lzyServerHost = "http://" + System.getenv(LZY_SERVER_HOST_ENV);
        } else {
            lzyServerHost = DEFAULT_LZY_SERVER_LOCALHOST;
        }
        URI serverURI = URI.create(lzyServerHost + ":" + port);

        try (ApplicationContext context = ApplicationContext.run(
            PropertySource.of(
                Map.of(
                    "server.server-uri", serverURI.toString()
                )
            )
        )) {
            Impl impl = context.getBean(Impl.class);
            ServerBuilder<?> builder = ServerBuilder.forPort(port)
                    .addService(impl);
            try{
                BackOfficeService backoffice = context.getBean(BackOfficeService.class);
                builder.addService(backoffice);
            }
            catch (NoSuchBeanException e){
                LOG.info("Running in inmemory mode without backoffice");
            }
            final Server server = builder.build();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("gRPC server is shutting down!");
                server.shutdown();
            }));
            server.awaitTermination();
        }
    }

    public static class Impl extends LzyServerGrpc.LzyServerImplBase {
        private final ZygoteRepository operations = new ZygoteRepositoryImpl();

        @Inject
        private ChannelsManager channels;

        @Inject
        private TasksManager tasks;

        @Inject
        private ConnectionManager connectionManager;

        @Inject
        private SessionManager sessionManager;

        @Inject
        private Authenticator auth;

        @Inject
        private StorageCredentialsProvider credentialsProvider;

        @Inject
        private StorageConfigs storageConfigs;

        @Override
        public void publish(Lzy.PublishRequest request, StreamObserver<Operations.RegisteredZygote> responseObserver) {
            LOG.info("Server::Publish " + JsonUtils.printRequest(request));
            final IAM.UserCredentials auth = request.getAuth();
            if (!this.auth.checkUser(auth.getUserId(), auth.getToken())) {
                responseObserver.onError(Status.ABORTED.asException());
                return;
            }
            if (!this.auth.canPublish(auth.getUserId())) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            final Operations.Zygote operation = request.getOperation();
            if (!operations.publish(request.getName(), gRPCConverter.from(operation))) {
                responseObserver.onError(Status.ALREADY_EXISTS.asException());
                return;
            }

            this.auth.registerOperation(request.getName(), auth.getUserId(), request.getScope());
            responseObserver.onNext(Operations.RegisteredZygote.newBuilder()
                .setWorkload(operation)
                .setName(request.getName())
                .build()
            );
            responseObserver.onCompleted();
        }

        @Override
        public void zygotes(IAM.Auth auth, StreamObserver<Operations.ZygoteList> responseObserver) {
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            final String user = resolveUser(auth);
            final Operations.ZygoteList.Builder builder = Operations.ZygoteList.newBuilder();
            operations.list().filter(op -> this.auth.canAccess(op, user)).forEach(zyName ->
                builder.addZygoteBuilder()
                    .setName(zyName)
                    .setWorkload(to(operations.get(zyName)))
                    .build()
            );
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void task(Tasks.TaskCommand request, StreamObserver<Tasks.TaskStatus> responseObserver) {
            if (!checkAuth(request.getAuth(), responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            Task task = null;
            switch (request.getCommandCase()) {
                case STATE:
                case SIGNAL: {
                    task = tasks.task(UUID.fromString(request.getTid()));
                    final Tasks.TaskSignal signal = request.getSignal();
                    if (task == null) {
                        responseObserver.onError(Status.NOT_FOUND.asException());
                        return;
                    }
                    if (!auth.canAccess(task, resolveUser(request.getAuth()))) {
                        responseObserver.onError(Status.PERMISSION_DENIED.asException());
                        return;
                    }
                    if (request.hasSignal()) {
                        task.signal(TasksManager.Signal.valueOf(signal.getSigValue()));
                    }
                    break;
                }
                case COMMAND_NOT_SET:
                    break;
            }
            if (task != null) {
                responseObserver.onNext(taskStatus(task, tasks));
                responseObserver.onCompleted();
            }
            else responseObserver.onError(new IllegalArgumentException());
        }

        @Override
        public void start(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            if (!checkAuth(request.getAuth(), responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            LOG.info("Server::start " + JsonUtils.printRequest(request));
            final Zygote workload = gRPCConverter.from(request.getZygote());
            final Map<Slot, String> assignments = new HashMap<>();
            request.getAssignmentsList().forEach(ass -> assignments.put(gRPCConverter.from(ass.getSlot()), ass.getBinding()));

            final String uid = resolveUser(request.getAuth());
            final Task parent = resolveTask(request.getAuth());
            final AtomicBoolean concluded = new AtomicBoolean(false);
            final SnapshotMeta snapshotMeta = request.hasSnapshotMeta() ? SnapshotMeta.from(request.getSnapshotMeta()) : null;
            Task task = tasks.start(uid, parent, workload, assignments, snapshotMeta, auth, progress -> {
                if (concluded.get()) {
                    return;
                }
                responseObserver.onNext(progress);
                if (progress.hasChanged() && progress.getChanged().getNewState() == Servant.StateChanged.State.DESTROYED) {
                    concluded.set(true);
                    responseObserver.onCompleted();
                    if (parent != null) {
                        parent.signal(TasksManager.Signal.CHLD);
                    }
                }
            }, auth.bucketForUser(uid));
            UserEventLogger.log(
                new UserEvent(
                    "Task created",
                    Map.of(
                        "task_id", task.tid().toString(),
                        "user_id", uid
                    ),
                    UserEvent.UserEventType.TaskCreate
                )
            );
            Context.current().addListener(ctxt -> {
                concluded.set(true);
                if (!EnumSet.of(FINISHED, DESTROYED).contains(task.state())) {
                    // TODO(d-kruchinin): Now we use raw stop of servant when connection to terminal was lost
                    // To make stopping process simple and understandable
                    task.stopServant();
                }
            }, Runnable::run);
        }

        @Override
        public void tasksStatus(IAM.Auth auth, StreamObserver<Tasks.TasksList> responseObserver) {
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            final String user = resolveUser(auth);
            final Tasks.TasksList.Builder builder = Tasks.TasksList.newBuilder();
            tasks.ps()
                .filter(t -> this.auth.canAccess(t, user))
                .map(t -> taskStatus(t, tasks)).forEach(builder::addTasks);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void channel(Channels.ChannelCommand request, StreamObserver<Channels.ChannelStatus> responseObserver) {
            LOG.info("Server::channel " + JsonUtils.printRequest(request));
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            Channel channel = null;
            switch (request.getCommandCase()) {
                case CREATE: {
                    final Channels.ChannelCreate create = request.getCreate();
                    channel = tasks.createChannel(
                        request.getChannelName(),
                        resolveUser(auth),
                        resolveTask(auth),
                        gRPCConverter.contentTypeFrom(create.getContentType()));
                    if (channel == null)
                        channel = channels.get(request.getChannelName());
                    break;
                }
                case DESTROY: {
                    channel = channels.get(request.getChannelName());
                    if (channel != null) {
                        channels.destroy(channel);
                        final Channels.ChannelStatus status = channelStatus(channel);
                        responseObserver.onNext(status);
                        responseObserver.onCompleted();
                        return;
                    }
                }
                case STATE: {
                    channel = channels.get(request.getChannelName());
                    break;
                }
            }
            if (channel != null) {
                responseObserver.onNext(channelStatus(channel));
                responseObserver.onCompleted();
            }
            else responseObserver.onError(Status.NOT_FOUND.asException());
        }

        @Override
        public void channelsStatus(IAM.Auth auth, StreamObserver<Channels.ChannelStatusList> responseObserver) {
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            final Channels.ChannelStatusList.Builder builder = Channels.ChannelStatusList.newBuilder();
            tasks.cs().forEach(channel -> builder.addStatuses(channelStatus(channel)));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void checkUserPermissions(Lzy.CheckUserPermissionsRequest request, StreamObserver<Lzy.CheckUserPermissionsResponse> responseObserver) {
            LOG.info("Server::checkPermissions " + JsonUtils.printRequest(request));
            IAM.Auth requestAuth = request.getAuth();
            if (!checkAuth(requestAuth, responseObserver)) {
                responseObserver.onNext(Lzy.CheckUserPermissionsResponse.newBuilder().setIsOk(false).build());
                responseObserver.onCompleted();
                return;
            }
            for (String permission: request.getPermissionsList()) {
                if (!auth.hasPermission(resolveUser(requestAuth), permission)) {
                    responseObserver.onNext(Lzy.CheckUserPermissionsResponse.newBuilder().setIsOk(false).build());
                    responseObserver.onCompleted();
                    LOG.info("User " + resolveUser(requestAuth) + " does not have permission " + permission);
                    return;
                }
            }
            responseObserver.onNext(Lzy.CheckUserPermissionsResponse.newBuilder().setIsOk(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void registerServant(Lzy.AttachServant request, StreamObserver<Lzy.AttachStatus> responseObserver) {
            LOG.info("Server::registerServant " + JsonUtils.printRequest(request));
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            final URI servantUri = URI.create(request.getServantURI());
            final UUID sessionId = UUID.fromString(request.getSessionId());
            final LzyServantGrpc.LzyServantBlockingStub servant = connectionManager.getOrCreate(servantUri, sessionId);
            responseObserver.onNext(Lzy.AttachStatus.newBuilder().build());
            responseObserver.onCompleted();

            ForkJoinPool.commonPool().execute(() -> {
                if (auth.hasTask()) {
                    final Task task = tasks.task(UUID.fromString(auth.getTask().getTaskId()));
                    task.attachServant(servantUri, servant);
                }
                else {
                    runTerminal(auth, servant, sessionId);
                }
                connectionManager.shutdownConnection(sessionId);
            });
        }

        @Override
        public void getS3Credentials(Lzy.GetS3CredentialsRequest request, StreamObserver<Lzy.GetS3CredentialsResponse> responseObserver) {
            LOG.info("Server::getS3Credentials " + JsonUtils.printRequest(request));
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            if (!this.auth.canAccessBucket(resolveUser(auth), request.getBucket())){
                responseObserver.onError(
                    Status.PERMISSION_DENIED.withDescription("Cannot access bucket " + request.getBucket())
                        .asException());
            }

            String uid = resolveUser(auth);

            String bucket = request.getBucket();

            StorageCredentials credentials =
                storageConfigs.isSeparated() ?
                    credentialsProvider.credentialsForBucket(uid, bucket) :
                    credentialsProvider.storageCredentials();
            responseObserver.onNext(to(credentials));
            responseObserver.onCompleted();
        }

        @Override
        public void getSessions(GetSessionsRequest request,
            StreamObserver<GetSessionsResponse> responseObserver) {
            final String userId = request.getAuth().getUserId();
            if (!auth.checkUser(userId, request.getAuth().getToken())) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
            }

            final Builder builder = GetSessionsResponse.newBuilder();
            sessionManager.sessionIds(userId).forEach(
                sessionId -> builder.addSessions(SessionDescription.newBuilder().setSessionId(sessionId.toString()))
            );
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getBucket(Lzy.GetBucketRequest request, StreamObserver<Lzy.GetBucketResponse> responseObserver) {
            LOG.info("Server::getBucket " + JsonUtils.printRequest(request));
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            String uid = resolveUser(auth);
            String bucket = this.auth.bucketForUser(uid);

            Lzy.GetBucketResponse response = Lzy.GetBucketResponse.newBuilder().setBucket(bucket).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        private void runTerminal(IAM.Auth auth, LzyServantGrpc.LzyServantBlockingStub kharon, UUID sessionId) {
            final String user = auth.getUser().getUserId();
            sessionManager.registerSession(user, sessionId);

            final Tasks.TaskSpec.Builder executionSpec = Tasks.TaskSpec.newBuilder();
            tasks.slots(user).forEach((slot, channel) -> executionSpec.addAssignmentsBuilder()
                .setSlot(to(slot))
                .setBinding("channel:" + channel.name())
                .build()
            );
            final Iterator<Servant.ExecutionProgress> execute = kharon.execute(executionSpec.build());
            try {
                execute.forEachRemaining(progress -> {
                    LOG.info("LzyServer::terminalProgress " + JsonUtils.printRequest(progress));
                    if (progress.hasAttach()) {
                        final Servant.SlotAttach attach = progress.getAttach();
                        final Slot slot = gRPCConverter.from(attach.getSlot());
                        final URI slotUri = URI.create(attach.getUri());
                        final String channelName = attach.getChannel();
                        tasks.addUserSlot(user, slot, channels.get(channelName));
                        this.channels.bind(channels.get(channelName), new ServantEndpoint(slot, slotUri, sessionId, kharon));
                    }
                    else if (progress.hasDetach()) {
                        final Servant.SlotDetach detach = progress.getDetach();
                        final Slot slot = gRPCConverter.from(detach.getSlot());
                        final URI slotUri = URI.create(detach.getUri());
                        tasks.removeUserSlot(user, slot);
                        final ServantEndpoint endpoint = new ServantEndpoint(slot, slotUri, sessionId, kharon);
                        final Channel bound = channels.bound(endpoint);
                        if (bound != null) {
                            channels.unbind(bound, endpoint);
                        }
                    }
                });
                LOG.info("Terminal for " + user + " disconnected");
            }
            catch (StatusRuntimeException th) {
                LOG.error("Terminal execution terminated ", th);
            }
            finally {
                LOG.info("unbindAll from runTerminal");
                //Clean up slots if terminal did not send detach
                tasks.slots(user).keySet().forEach(slot -> tasks.removeUserSlot(user, slot));
                channels.unbindAll(sessionId);
                tasks.destroyUserChannels(user);
                sessionManager.deleteSession(user, sessionId);
            }
        }

        public static Tasks.TaskStatus taskStatus(Task task, TasksManager tasks) {
            final Tasks.TaskStatus.Builder builder = Tasks.TaskStatus.newBuilder();
            try {
                builder.setTaskId(task.tid().toString());

                builder.setZygote(
                        ((AtomicZygote)task.workload()).zygote()
                );

                builder.setStatus(Tasks.TaskStatus.Status.valueOf(task.state().toString()));
                if (task.servantUri() != null) {
                    builder.setServant(task.servantUri().toString());
                }
                builder.setOwner(tasks.owner(task.tid()));
                Stream.concat(Stream.of(task.workload().input()), Stream.of(task.workload().output()))
                    .map(task::slotStatus)
                    .forEach(slotStatus -> {
                        final Operations.SlotStatus.Builder slotStateBuilder = builder.addConnectionsBuilder();
                        slotStateBuilder.setTaskId(task.tid().toString());
                        slotStateBuilder.setDeclaration(to(slotStatus.slot()));
                        URI connected = slotStatus.connected();
                        if (connected != null)
                            slotStateBuilder.setConnectedTo(connected.toString());
                        slotStateBuilder.setPointer(slotStatus.pointer());
                        LOG.info("Getting status of slot with state: " + slotStatus.state().name());
                        slotStateBuilder.setState(Operations.SlotStatus.State.valueOf(slotStatus.state().name()));
                        builder.addConnections(slotStateBuilder.build());
                    });
            }
            catch (TaskException te) {
                builder.setExplanation(te.getMessage());
            }

            return builder.build();
        }

        private Channels.ChannelStatus channelStatus(Channel channel) {
            final Channels.ChannelStatus.Builder slotStatus = Channels.ChannelStatus.newBuilder();
            slotStatus.setChannel(to(channel));
            for (SlotStatus state : tasks.connected(channel)) {
                final Operations.SlotStatus.Builder builder = slotStatus.addConnectedBuilder();
                if (state.tid() != null) {
                    builder.setTaskId(state.tid().toString());
                    builder.setUser(tasks.owner(state.tid()));
                }
                else builder.setUser(state.user());

                builder.setConnectedTo(channel.name())
                    .setDeclaration(to(state.slot()))
                    .setPointer(state.pointer())
                    .setState(Operations.SlotStatus.State.valueOf(state.state().toString()))
                    .build();
            }
            return slotStatus.build();
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        private boolean checkAuth(IAM.Auth auth, StreamObserver<?> responseObserver) {
            if (auth == null) {
                responseObserver.onError(Status.INVALID_ARGUMENT.asException());
                return false;
            }
            else if (auth.hasUser()) {
                return this.auth.checkUser(auth.getUser().getUserId(), auth.getUser().getToken());
            }
            else if (auth.hasTask()) {
                return this.auth.checkTask(auth.getTask().getTaskId(), auth.getTask().getToken());
            }
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());

            return false;
        }

        private String resolveUser(IAM.Auth auth) {
            LOG.info("Resolving user for auth " + JsonUtils.printRequest(auth));
            return auth.hasUser() ? auth.getUser().getUserId() : this.auth.userForTask(resolveTask(auth));
        }

        private Task resolveTask(IAM.Auth auth) {
            return auth.hasTask() ? tasks.task(UUID.fromString(auth.getTask().getTaskId())) : null;
        }
    }
}
