package ru.yandex.cloud.ml.platform.lzy.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Channel;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;
import ru.yandex.cloud.ml.platform.lzy.server.local.LocalChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.local.LocalTasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.mem.SimpleInMemAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.server.mem.ZygoteRepositoryImpl;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static ru.yandex.cloud.ml.platform.lzy.server.task.Task.State.DESTROYED;
import static ru.yandex.cloud.ml.platform.lzy.server.task.Task.State.FINISHED;

public class LzyServer {
    private static final Logger LOG = LogManager.getLogger(LzyServer.class);

    private static final Options options = new Options();
    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
    }

    private static int port;

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
        port = Integer.parseInt(parse.getOptionValue('p', "8888"));

        final Server server = ServerBuilder.forPort(port).addService(new Impl()).build();

        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("gRPC server is shutting down!");
            server.shutdown();
        }));
        server.awaitTermination();
    }

    public static class Impl extends LzyServerGrpc.LzyServerImplBase {
        private final ZygoteRepository operations = new ZygoteRepositoryImpl();
        private final ChannelsRepository channels = new LocalChannelsRepository();
        private final TasksManager tasks = new LocalTasksManager(URI.create("http://localhost:" + port), channels);
        private final Authenticator auth = new SimpleInMemAuthenticator();

        @Override
        public void publish(Lzy.PublishRequest request, StreamObserver<Operations.RegisteredZygote> responseObserver) {
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
                    .setWorkload(gRPCConverter.to(operations.get(zyName)))
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
                responseObserver.onNext(taskStatus(task));
                responseObserver.onCompleted();
            }
            else responseObserver.onError(new IllegalArgumentException());
        }

        @Override
        public void start(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            final Zygote workload = gRPCConverter.from(request.getZygote());
            final Map<Slot, String> assignments = new HashMap<>();
            request.getAssignmentsList().forEach(ass -> assignments.put(gRPCConverter.from(ass.getSlot()), ass.getBinding()));

            final String uid = resolveUser(request.getAuth());
            final Task parent = resolveTask(request.getAuth());
            final AtomicBoolean concluded = new AtomicBoolean(false);
            Task task = tasks.start(uid, parent, workload, assignments, auth, progress -> {
                if (concluded.get())
                    return;
                try {
                    LOG.info(JsonFormat.printer().print(progress));
                    responseObserver.onNext(progress);
                    if (progress.hasChanged() && progress.getChanged().getNewState() == Servant.StateChanged.State.DESTROYED) {
                        concluded.set(true);
                        responseObserver.onCompleted();
                        if (parent != null)
                            parent.signal(TasksManager.Signal.CHLD);
                    }
                } catch (InvalidProtocolBufferException ignore) {}
            });
            Context.current().addListener(ctxt -> {
                concluded.set(true);
                if (!EnumSet.of(FINISHED, DESTROYED).contains(task.state()))
                    task.signal(TasksManager.Signal.TERM);
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
                .map(this::taskStatus).forEach(builder::addTasks);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void channel(Channels.ChannelCommand request, StreamObserver<Channels.ChannelStatus> responseObserver) {
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
                        gRPCConverter.contentTypeFrom(create.getContentType())
                    );
                    if (channel == null)
                        channel = channels.get(request.getChannelName());
                    break;
                }
                case DESTROY: {
                    channel = channels.get(request.getChannelName());
                    if (channel != null) {
                        final Channels.ChannelStatus status = channelStatus(channel);
                        channels.destroy(channel);
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
        public void registerServant(Lzy.AttachServant request, StreamObserver<Lzy.AttachStatus> responseObserver) {
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver)) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }

            final URI servantUri = URI.create(request.getServantURI());
            responseObserver.onNext(Lzy.AttachStatus.newBuilder().build());
            responseObserver.onCompleted();

            ForkJoinPool.commonPool().execute(() -> {
                if (auth.hasTask()) {
                    final Task task = tasks.task(UUID.fromString(auth.getTask().getTaskId()));
                    task.attachServant(servantUri);
                }
                else runTerminal(auth, servantUri);
            });
        }

        private void runTerminal(IAM.Auth auth, URI servantUri) {
            final String user = auth.getUser().getUserId();
            final ManagedChannel servantChannel = ManagedChannelBuilder
                .forAddress(servantUri.getHost(), servantUri.getPort())
                .usePlaintext()
                .build();
            final Tasks.TaskSpec.Builder executionSpec = Tasks.TaskSpec.newBuilder();
            tasks.slots(user).forEach((slot, channel) -> executionSpec.addAssignmentsBuilder()
                .setSlot(gRPCConverter.to(slot))
                .setBinding("channel:" + channel.name())
                .build()
            );
            final Iterator<Servant.ExecutionProgress> execute = LzyServantGrpc
                .newBlockingStub(servantChannel)
                .execute(executionSpec.build());
            try {
                execute.forEachRemaining(progress -> {
                    try {
                        LOG.info(JsonFormat.printer().print(progress));
                    } catch (InvalidProtocolBufferException e) {
                        LOG.error("Unable to parse progress", e);
                    }
                    if (progress.hasAttach()) {
                        final Servant.SlotAttach attach = progress.getAttach();
                        final Slot slot = gRPCConverter.from(attach.getSlot());
                        final URI slotUri = URI.create(attach.getUri());
                        final String channelName = attach.getChannel();
                        tasks.addUserSlot(user, slot, channels.get(channelName));
                        this.channels.bind(channels.get(channelName), Binding.singleton(slot, slotUri, servantChannel));
                    }
                    else if (progress.hasDetach()) {
                        final Servant.SlotDetach detach = progress.getDetach();
                        final Slot slot = gRPCConverter.from(detach.getSlot());
                        final URI slotUri = URI.create(detach.getUri());
                        tasks.removeUserSlot(user, slot);
                        final Channel bound = this.channels.bound(slotUri);
                        if (bound != null) {
                            final Binding binding = Binding.singleton(slot, slotUri, servantChannel);
                            binding.invalidate();
                            channels.unbind(bound, binding);
                        }
                    }
                });
                LOG.info("Terminal for " + user + " disconnected");
            }
            catch (StatusRuntimeException th) {
                LOG.error("Terminal execution terminated ", th);
            }
            finally {
                channels.unbindAll(servantUri);
                servantChannel.shutdown();
            }
        }

        private Tasks.TaskStatus taskStatus(Task task) {
            final Tasks.TaskStatus.Builder builder = Tasks.TaskStatus.newBuilder();
            try {
                builder.setTaskId(task.tid().toString());

                builder.setStatus(Tasks.TaskStatus.Status.valueOf(task.state().toString()));
                if (task.servant() != null)
                    builder.setServant(task.servant().toString());
                builder.setOwner(tasks.owner(task.tid()));
                Stream.concat(Stream.of(task.workload().input()), Stream.of(task.workload().output()))
                    .map(task::slotStatus)
                    .forEach(slotStatus -> {
                        final Operations.SlotStatus.Builder slotStateBuilder = builder.addConnectionsBuilder();
                        slotStateBuilder.setTaskId(task.tid().toString());
                        slotStateBuilder.setDeclaration(gRPCConverter.to(slotStatus.slot()));
                        slotStateBuilder.setConnectedTo(slotStatus.connected().toString());
                        slotStateBuilder.setPointer(slotStatus.pointer());
                        slotStateBuilder.setState(Operations.SlotStatus.State.valueOf(slotStatus.state().toString()));
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
            slotStatus.setChannel(gRPCConverter.to(channel));
            for (SlotStatus state : tasks.connected(channel)) {
                final Operations.SlotStatus.Builder builder = slotStatus.addConnectedBuilder();
                if (state.tid() != null) {
                    builder.setTaskId(state.tid().toString());
                    builder.setUser(tasks.owner(state.tid()));
                }
                else builder.setUser(state.user());

                builder.setConnectedTo(channel.name())
                    .setDeclaration(gRPCConverter.to(state.slot()))
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
            return auth.hasUser() ? auth.getUser().getUserId() : this.auth.userForTask(resolveTask(auth));
        }

        private Task resolveTask(IAM.Auth auth) {
            return auth.hasTask() ? tasks.task(UUID.fromString(auth.getTask().getTaskId())) : null;
        }
    }
}
