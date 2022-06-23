package ai.lzy.server;

import static ai.lzy.model.GrpcConverter.from;
import static ai.lzy.model.GrpcConverter.to;
import static ai.lzy.priv.v2.Tasks.TaskProgress.Status.ERROR;
import static ai.lzy.priv.v2.Tasks.TaskProgress.Status.QUEUE;
import static ai.lzy.priv.v2.Tasks.TaskProgress.Status.SUCCESS;

import ai.lzy.model.Constants;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.Slot;
import ai.lzy.model.SlotStatus;
import ai.lzy.model.StorageCredentials;
import ai.lzy.model.Zygote;
import ai.lzy.model.channel.ChannelSpec;
import ai.lzy.model.channel.DirectChannelSpec;
import ai.lzy.model.channel.SnapshotChannelSpec;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.AtomicZygote;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.logs.UserEvent;
import ai.lzy.model.logs.UserEventLogger;
import ai.lzy.model.utils.SessionIdInterceptor;
import ai.lzy.server.configs.StorageConfigs;
import ai.lzy.server.local.ServantEndpoint;
import ai.lzy.server.mem.ZygoteRepositoryImpl;
import ai.lzy.server.storage.StorageCredentialsProvider;
import ai.lzy.server.task.Task;
import ai.lzy.server.task.TaskException;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.exceptions.NoSuchBeanException;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import ai.lzy.priv.v2.Channels;
import ai.lzy.priv.v2.Channels.ChannelCreate;
import ai.lzy.priv.v2.Channels.ChannelStatus;
import ai.lzy.priv.v2.IAM;
import ai.lzy.priv.v2.IAM.Auth;
import ai.lzy.priv.v2.Lzy;
import ai.lzy.priv.v2.Lzy.GetSessionsRequest;
import ai.lzy.priv.v2.Lzy.GetSessionsResponse;
import ai.lzy.priv.v2.Lzy.GetSessionsResponse.Builder;
import ai.lzy.priv.v2.Lzy.SessionDescription;
import ai.lzy.priv.v2.LzyFsApi;
import ai.lzy.priv.v2.LzyFsGrpc;
import ai.lzy.priv.v2.LzyServantGrpc;
import ai.lzy.priv.v2.LzyServerGrpc;
import ai.lzy.priv.v2.Operations;
import ai.lzy.priv.v2.Servant;
import ai.lzy.priv.v2.Tasks;
import ai.lzy.priv.v2.Tasks.TaskSignal;

public class LzyServer {

    private static final Logger LOG;
    private static final Options options = new Options();
    private static final String LZY_SERVER_HOST_ENV = "LZY_SERVER_HOST";
    private static final String DEFAULT_LZY_SERVER_LOCALHOST = "http://localhost";

    static {
        // This is to avoid this bug: https://issues.apache.org/jira/browse/LOG4J2-2375
        // KafkaLogsConfiguration will fall, so then we must call reconfigure
        ProducerConfig.configNames();
        LoggerContext ctx = (LoggerContext) LogManager.getContext();
        ctx.reconfigure();
        LOG = LogManager.getLogger(LzyServer.class);
    }

    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
    }

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
            ServerBuilder<?> builder = NettyServerBuilder.forPort(port)
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
                .addService(ServerInterceptors.intercept(impl, new SessionIdInterceptor()));
            try {
                BackOfficeService backoffice = context.getBean(BackOfficeService.class);
                builder.addService(backoffice);
            } catch (NoSuchBeanException e) {
                LOG.info("Running in inmemory mode without backoffice");
            }
            final Server server = builder.build();
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("gRPC server is shutting down!");
                server.shutdown();
                impl.close();
            }));
            server.awaitTermination();
        }
    }

    public static class Impl extends LzyServerGrpc.LzyServerImplBase {

        public static final ThreadGroup TERMINAL_THREADS = new ThreadGroup("Terminal threads");
        private final ZygoteRepository operations = new ZygoteRepositoryImpl();
        private final Set<Thread> terminalThreads = new HashSet<>();

        @Inject
        private ChannelsManager channels;

        @Inject
        private TasksManager tasks;

        @Inject
        private ServantsAllocator.Ex servantsAllocator;

        @Inject
        private Authenticator auth;

        @Inject
        private StorageCredentialsProvider credentialsProvider;

        @Inject
        private StorageConfigs storageConfigs;

        public static Tasks.TaskStatus taskStatus(Task task, TasksManager tasks) {
            final Tasks.TaskStatus.Builder builder = Tasks.TaskStatus.newBuilder();
            try {
                builder.setTaskId(task.tid());

                builder.setZygote(
                    ((AtomicZygote) task.workload()).zygote()
                );

                builder.setStatus(Tasks.TaskProgress.Status.valueOf(task.state().toString()));
                final URI uri = task.servantUri();
                if (uri != null) {
                    builder.setServant(uri.toString());
                }
                builder.setOwner(tasks.owner(task.tid()));
                Stream.concat(Stream.of(task.workload().input()), Stream.of(task.workload().output()))
                    .map(task::slotStatus)
                    .forEach(slotStatus -> {
                        final Operations.SlotStatus.Builder slotStateBuilder = builder.addConnectionsBuilder();
                        slotStateBuilder.setTaskId(task.tid());
                        slotStateBuilder.setDeclaration(to(slotStatus.slot()));
                        URI connected = slotStatus.connected();
                        if (connected != null) {
                            slotStateBuilder.setConnectedTo(connected.toString());
                        }
                        slotStateBuilder.setPointer(slotStatus.pointer());
                        LOG.info("Getting status of slot with state: " + slotStatus.state().name());
                        slotStateBuilder.setState(Operations.SlotStatus.State.valueOf(slotStatus.state().name()));
                        builder.addConnections(slotStateBuilder.build());
                    });
            } catch (TaskException te) {
                builder.setExplanation(te.getMessage());
            }

            return builder.build();
        }

        @Override
        public void publish(Lzy.PublishRequest request, StreamObserver<Lzy.PublishResponse> responseObserver) {
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
            if (!operations.publish(operation.getName(), from(operation))) {
                responseObserver.onError(Status.ALREADY_EXISTS.asException());
                return;
            }

            this.auth.registerOperation(operation.getName(), auth.getUserId(), request.getScope());
            responseObserver.onNext(Lzy.PublishResponse.getDefaultInstance());
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
                    .mergeFrom(to(operations.get(zyName)))
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
                    task = tasks.task(request.getTid());
                    final TaskSignal signal = request.getSignal();
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
                default:
                    throw new IllegalStateException("Unexpected value: " + request.getCommandCase());
            }
            if (task != null) {
                responseObserver.onNext(taskStatus(task, tasks));
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new IllegalArgumentException());
            }
        }

        @Override
        public void start(Tasks.TaskSpec request, StreamObserver<Tasks.TaskProgress> responseObserver) {
            if (!checkAuth(request.getAuth(), responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            if (LOG.getLevel().isLessSpecificThan(Level.DEBUG)) {
                LOG.debug("Server::start " + JsonUtils.printRequest(request));
            } else {
                LOG.info("Server::start request (tid={})", request.getTid());
            }
            final Operations.Zygote zygote = request.getZygote();
            final Zygote workload = from(zygote);
            final Map<Slot, String> assignments = new HashMap<>();
            request.getAssignmentsList()
                .forEach(ass -> assignments.put(from(ass.getSlot()), ass.getBinding()));

            final String uid = resolveUser(request.getAuth());
            final Task parent = resolveTask(request.getAuth());
            final SessionManager.Session session = resolveSession(request.getAuth());
            final AtomicBoolean concluded = new AtomicBoolean(false);
            // [TODO] session per user is too simple
            final Task task = tasks.start(uid, parent, workload, assignments, auth);
            task.onProgress(progress -> {
                if (concluded.get()) {
                    return;
                }
                responseObserver.onNext(progress);
                if (progress.getStatus() == QUEUE) {
                    final String sessionId = session.id();
                    servantsAllocator.allocate(sessionId, from(zygote.getProvisioning()), from(zygote.getEnv()))
                        .whenComplete((connection, th) -> {
                            if (th != null) {
                                if (th instanceof EnvironmentInstallationException) {
                                    LOG.info("Env installation failed, uid={}, tid={}", uid, task.tid(), th);
                                    task.state(Task.State.ERROR, ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc(),
                                        th.getMessage(), Arrays.toString(th.getStackTrace()));
                                } else {
                                    LOG.error("Servant allocation error, uid={}, tid={}", uid, task.tid(), th);
                                    task.state(Task.State.ERROR, ReturnCodes.INTERNAL_ERROR.getRc(),
                                        "Internal error");
                                }
                            } else {
                                task.attachServant(connection);
                                auth.registerTask(uid, task, connection.id());
                            }
                        });
                } else if (EnumSet.of(ERROR, SUCCESS).contains(progress.getStatus())) {
                    concluded.set(true);
                    responseObserver.onCompleted();
                    if (parent != null) {
                        parent.signal(TasksManager.Signal.CHLD);
                    }
                }
            });
            UserEventLogger.log(
                new UserEvent(
                    "Task created",
                    Map.of(
                        "task_id", task.tid(),
                        "user_id", uid
                    ),
                    UserEvent.UserEventType.TaskCreate
                )
            );
            Context.current().addListener(ctxt -> {
                concluded.set(true);
                try {
                    if (task.state().phase() < Task.State.SUCCESS.phase()) { // task is not complete yet
                        task.signal(TasksManager.Signal.HUB);
                    }
                } catch (TaskException e) {
                    LOG.warn("Exception during HUB signal, task id={}", task.tid(), e);
                }
            }, Runnable::run);
            task.state(Task.State.QUEUE);
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

            ChannelSpec channel;
            switch (request.getCommandCase()) {
                case CREATE: {
                    final ChannelCreate create = request.getCreate();
                    final ChannelSpec spec;
                    switch (create.getTypeCase()) {
                        case SNAPSHOT: {
                            spec = new SnapshotChannelSpec(
                                request.getChannelName(),
                                GrpcConverter.contentTypeFrom(create.getContentType()),
                                create.getSnapshot().getSnapshotId(),
                                create.getSnapshot().getEntryId(),
                                request.getAuth()
                            );
                            break;

                        }
                        case DIRECT: {
                            spec = new DirectChannelSpec(request.getChannelName(),
                                GrpcConverter.contentTypeFrom(create.getContentType()));
                            break;
                        }
                        default:
                            throw new NotImplementedException();
                    }
                    channel = tasks.createChannel(
                        resolveUser(auth),
                        resolveTask(auth),
                        spec
                    );
                    if (channel == null) {
                        channel = channels.get(request.getChannelName());
                    }
                    break;
                }
                case DESTROY: {
                    channel = channels.get(request.getChannelName());
                    if (channel != null) {
                        channels.destroy(channel);
                        final ChannelStatus status = channelStatus(channel);
                        responseObserver.onNext(status);
                        responseObserver.onCompleted();
                        return;
                    }
                    break;
                }
                case STATE: {
                    channel = channels.get(request.getChannelName());
                    break;
                }
                default:
                    throw new IllegalStateException("Unexpected value: " + request.getCommandCase());
            }
            if (channel != null) {
                responseObserver.onNext(channelStatus(channel));
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.NOT_FOUND.asException());
            }
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
        public void checkUserPermissions(Lzy.CheckUserPermissionsRequest request,
                                         StreamObserver<Lzy.CheckUserPermissionsResponse> responseObserver) {
            LOG.info("Server::checkPermissions " + JsonUtils.printRequest(request));
            IAM.Auth requestAuth = request.getAuth();
            if (!checkAuth(requestAuth, responseObserver)) {
                responseObserver.onNext(Lzy.CheckUserPermissionsResponse.newBuilder().setIsOk(false).build());
                responseObserver.onCompleted();
                return;
            }
            for (String permission : request.getPermissionsList()) {
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
            final URI fsUri = URI.create(request.getFsURI());

            if (auth.hasTask()) {
                final String servantId = auth.getTask().getServantId();
                servantsAllocator.register(servantId, servantUri, fsUri);
            } else {
                final Thread terminalThread = new Thread(
                    TERMINAL_THREADS,
                    () -> {
                        final String sessionId = request.getServantId();
                        Context.current().addListener(ctxt -> Thread.currentThread().interrupt(), Runnable::run);

                        final ManagedChannel servantChannel = ChannelBuilder
                            .forAddress(servantUri.getHost(), servantUri.getPort())
                            .usePlaintext()
                            .enableRetry(LzyServantGrpc.SERVICE_NAME)
                            .build();

                        final ManagedChannel fsChannel = ChannelBuilder
                            .forAddress(fsUri.getHost(), fsUri.getPort())
                            .usePlaintext()
                            .enableRetry(LzyFsGrpc.SERVICE_NAME)
                            .build();

                        final Metadata metadata = new Metadata();
                        metadata.put(Constants.SESSION_ID_METADATA_KEY, sessionId);

                        final LzyServantGrpc.LzyServantBlockingStub servant = MetadataUtils
                            .attachHeaders(LzyServantGrpc.newBlockingStub(servantChannel), metadata);
                        final LzyFsGrpc.LzyFsBlockingStub fs = MetadataUtils
                            .attachHeaders(LzyFsGrpc.newBlockingStub(fsChannel), metadata);

                        try {
                            runTerminal(auth, servant, fs, sessionId);
                        } finally {
                            servantChannel.shutdownNow();
                            fsChannel.shutdownNow();
                            terminalThreads.remove(Thread.currentThread());
                        }
                    },
                    "Terminal " + request.getServantId() + " for user " + auth.getUser().getUserId()
                );
                terminalThread.setDaemon(true);
                terminalThread.start();
                terminalThreads.add(terminalThread);
            }
            responseObserver.onNext(Lzy.AttachStatus.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void getS3Credentials(Lzy.GetS3CredentialsRequest request,
                                     StreamObserver<Lzy.GetS3CredentialsResponse> responseObserver) {
            LOG.info("Server::getS3Credentials " + JsonUtils.printRequest(request));
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            final String owner;
            if (!auth.hasUser()) {
                final String servantId = auth.getTask().getServantId();
                final SessionManager.Session session = servantsAllocator.byServant(servantId);
                if (session == null) {
                    LOG.warn("Astray servant found: " + servantId);
                    responseObserver.onError(Status.INVALID_ARGUMENT.asException());
                    return;
                }
                owner = session.owner();
            } else {
                owner = auth.getUser().getUserId();
            }
            if (!this.auth.canAccessBucket(owner, request.getBucket())) {
                responseObserver.onError(
                    Status.PERMISSION_DENIED.withDescription("Cannot access bucket " + request.getBucket())
                        .asException());
            }
            final String bucket = request.getBucket();

            final StorageCredentials credentials =
                storageConfigs.isSeparated()
                    ? credentialsProvider.credentialsForBucket(owner, bucket) :
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
            servantsAllocator.sessions(userId).forEach(
                s -> builder.addSessions(SessionDescription.newBuilder().setSessionId(s.id()))
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

        @Override
        public void getUser(Lzy.GetUserRequest request,
                            StreamObserver<Lzy.GetUserResponse> responseObserver) {
            final Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            String uid = resolveUser(auth);
            if (uid == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            Lzy.GetUserResponse response = Lzy.GetUserResponse.newBuilder().setUserId(uid).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * [TODO] support interruption
         */
        private void runTerminal(IAM.Auth auth, LzyServantGrpc.LzyServantBlockingStub terminalServant,
                                 LzyFsGrpc.LzyFsBlockingStub terminalFs, String sessionId) {
            final String user = auth.getUser().getUserId();
            servantsAllocator.registerSession(user, sessionId, this.auth.bucketForUser(user));

            final Iterator<Servant.ServantProgress> start = terminalServant.start(IAM.Empty.newBuilder().build());

            tasks.slots(user).forEach((slot, channel) -> {
                final LzyFsApi.SlotCommand slotCommand = LzyFsApi.SlotCommand.newBuilder()
                    .setCreate(LzyFsApi.CreateSlotCommand.newBuilder()
                        .setSlot(GrpcConverter.to(slot))
                        .setChannelId(channel.name())
                        .build())
                    .setSlot(slot.name())
                    .setTid(auth.hasTask() ? auth.getTask().getTaskId() : "terminal-" + auth.getUser().getUserId())
                    .build();
                final LzyFsApi.SlotCommandStatus status = terminalFs.configureSlot(slotCommand);

                if (status.hasRc() && status.getRc().getCode() != LzyFsApi.SlotCommandStatus.RC.Code.SUCCESS) {
                    LOG.error("Unable to configure terminal servant slot. session: {}, slot: {}, error: {}",
                        sessionId, slot, status.getRc().toString());
                }
            });

            try {
                start.forEachRemaining(progress -> {
                    LOG.info("LzyServer::terminalProgress " + JsonUtils.printRequest(progress));
                    if (progress.hasAttach()) {
                        final Servant.SlotAttach attach = progress.getAttach();
                        final Slot slot = from(attach.getSlot());
                        final URI slotUri = URI.create(attach.getUri());
                        final String channelName = attach.getChannel();
                        tasks.addUserSlot(user, slot, channels.get(channelName));
                        this.channels.bind(channels.get(channelName),
                            new ServantEndpoint(slot, slotUri, sessionId, terminalFs));
                    } else if (progress.hasDetach()) {
                        final Servant.SlotDetach detach = progress.getDetach();
                        final Slot slot = from(detach.getSlot());
                        final URI slotUri = URI.create(detach.getUri());
                        tasks.removeUserSlot(user, slot);
                        final ServantEndpoint endpoint = new ServantEndpoint(slot, slotUri, sessionId, terminalFs);
                        final ChannelSpec bound = channels.bound(endpoint);
                        if (bound != null) {
                            channels.unbind(bound, endpoint);
                        }
                    }
                });
                LOG.info("Terminal for " + user + " disconnected");
            } catch (StatusRuntimeException th) {
                LOG.error("Terminal execution terminated ", th);
            } finally {
                LOG.info("unbindAll from runTerminal");
                //Clean up slots if terminal did not send detach
                tasks.slots(user).keySet().forEach(slot -> tasks.removeUserSlot(user, slot));
                channels.unbindAll(sessionId);
                tasks.destroyUserChannels(user); // o_O
                servantsAllocator.deleteSession(sessionId);
            }
        }

        private Channels.ChannelStatus channelStatus(ChannelSpec channel) {
            final Channels.ChannelStatus.Builder slotStatus = Channels.ChannelStatus.newBuilder();
            slotStatus.setChannel(to(channel));
            for (SlotStatus state : tasks.connected(channel)) {
                final Operations.SlotStatus.Builder builder = slotStatus.addConnectedBuilder();
                if (state.tid() != null) {
                    builder.setTaskId(state.tid());
                    builder.setUser(tasks.owner(state.tid()));
                } else {
                    builder.setUser(state.user());
                }

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
            } else if (auth.hasUser()) {
                return this.auth.checkUser(auth.getUser().getUserId(), auth.getUser().getToken());
            } else if (auth.hasTask()) {
                final IAM.TaskCredentials task = auth.getTask();
                return this.auth.checkTask(
                    task.getTaskId().isEmpty() ? null : task.getTaskId(),
                    task.getServantId(),
                    task.getServantToken()
                );
            }
            responseObserver.onError(Status.INVALID_ARGUMENT.asException());

            return false;
        }

        private String resolveUser(IAM.Auth auth) {
            return resolveSession(auth).owner();
        }

        private Task resolveTask(IAM.Auth auth) {
            return auth.hasTask() ? tasks.task(auth.getTask().getTaskId()) : null;
        }

        private SessionManager.Session resolveSession(IAM.Auth auth) {
            if (auth.hasTask()) {
                return servantsAllocator.byServant(auth.getTask().getServantId());
            } else {
                return servantsAllocator.userSession(auth.getUser().getUserId());
            }
        }

        public void close() {
            terminalThreads.forEach(Thread::interrupt);
        }
    }
}
