package ru.yandex.cloud.ml.platform.lzy.server;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
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
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.local.Binding;
import ru.yandex.cloud.ml.platform.lzy.server.local.LocalChannelsRepository;
import ru.yandex.cloud.ml.platform.lzy.server.local.LocalTask;
import ru.yandex.cloud.ml.platform.lzy.server.local.LocalTasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.mem.SimpleInMemAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.server.mem.ZygoteRepositoryImpl;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class LzyServer {
    private static final Logger LOG = LogManager.getLogger(LocalTask.class);

    private static final Options options = new Options();
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
        private final TasksManager tasks = new LocalTasksManager(channels);
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
                .setWorkload(request.getOperation())
                .build()
            );
            responseObserver.onCompleted();
        }

        @Override
        public void zygotes(IAM.Auth auth, StreamObserver<Operations.ZygoteList> responseObserver) {
            if (!checkAuth(auth, responseObserver))
                return;

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
            if (!checkAuth(request.getAuth(), responseObserver))
                return;

            Task task = null;
            switch (request.getCommandCase()) {
                case CREATE: {
                    final Tasks.TaskCreate create = request.getCreate();
                    final Zygote workload = operations.get(create.getZygote());
                    final Map<Slot, Channel> assignments = new HashMap<>();
                    create.getAssignmentsList().forEach(ass ->
                        assignments.put(workload.slot(ass.getSlotName()), tasks.channel(ass.getChannelId()))
                    );

                    final String uid = resolveUser(request.getAuth());
                    task = tasks.start(uid, resolveTask(request.getAuth()), workload, assignments, auth);
                    break;
                }
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
        public void tasksStatus(IAM.Auth auth, StreamObserver<Tasks.TasksList> responseObserver) {
            if (!checkAuth(auth, responseObserver))
                return;

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
            if (!checkAuth(auth, responseObserver))
                return;

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
                    if (channel == null) {
                        responseObserver.onError(Status.ALREADY_EXISTS.asException());
                        return;
                    }
                    break;
                }
                case DESTROY: {
                    break;
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
            else responseObserver.onError(new IllegalArgumentException());
        }

        @Override
        public void channelsStatus(IAM.Auth auth, StreamObserver<Channels.ChannelStatusList> responseObserver) {
            if (!checkAuth(auth, responseObserver))
                return;

            final Channels.ChannelStatusList.Builder builder = Channels.ChannelStatusList.newBuilder();
            tasks.cs().forEach(channel -> builder.addStatuses(channelStatus(channel)));
        }

        @Override
        public void registerServant(Lzy.AttachServant request, StreamObserver<Lzy.AttachStatus> responseObserver) {
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver))
                return;
            responseObserver.onNext(Lzy.AttachStatus.newBuilder().build());
            responseObserver.onCompleted();
            new Thread(() -> {
                final URI servantUri = URI.create(request.getServantURI());
                if (auth.hasTask()) {
                    final Task task = tasks.task(UUID.fromString(auth.getTask().getTaskId()));
                    task.attachServant(servantUri);
                } else if (auth.hasUser()) {
                    final String user = auth.getUser().getUserId();
                    final ManagedChannel servantChannel = ManagedChannelBuilder
                        .forAddress(servantUri.getHost(), servantUri.getPort())
                        .usePlaintext()
                        .build();
                    final Servant.ExecutionSpec.Builder executionSpec = Servant.ExecutionSpec.newBuilder();
                    tasks.slots(user).forEach((slot, channel) -> executionSpec.addSlotsBuilder()
                        .setSlot(gRPCConverter.to(slot))
                        .setChannelId(channel.name())
                        .build()
                    );
                    final Iterator<Servant.ExecutionProgress> execute = LzyServantGrpc
                        .newBlockingStub(servantChannel)
                        .execute(executionSpec.build());
                    new Thread(() -> {
                        try {
                            execute.forEachRemaining(progress -> {
                                if (progress.hasAttached()) {
                                    final Servant.AttachSlot attached = progress.getAttached();
                                    final Slot slot = gRPCConverter.from(attached.getSlot());
                                    tasks.setSlot(user, slot, channels.get(attached.getChannel()));
                                    final Binding binding = new Binding(
                                        slot,
                                        servantUri.resolve(slot.name()),
                                        servantChannel
                                    );
                                    this.channels.bind(channels.get(attached.getChannel()), binding);
                                }
                                try {
                                    LOG.info(JsonFormat.printer().print(progress));
                                } catch (InvalidProtocolBufferException ignore) {
                                }
                            });
                        }
                        catch (Exception ignore) {}
                        finally {
                            channels.unbindAll(servantUri);
                            LOG.info("Terminal for " + user + " disconnected");
                        }
                    }, "Lzy terminal " + user).start();
                }
            }).start();
        }

        private Tasks.TaskStatus taskStatus(Task task) {
            final Tasks.TaskStatus.Builder builder = Tasks.TaskStatus.newBuilder();
            try {
                builder.setTaskId(task.tid().toString());

                builder.setStatus(Tasks.TaskStatus.Status.valueOf(task.state().toString()));
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
                slotStatus.addConnectedBuilder()
                    .setTaskId(state.task().tid().toString())
                    .setConnectedTo(channel.name())
                    .setDeclaration(gRPCConverter.to(state.slot()))
                    .setPointer(state.pointer())
                    .setState(Operations.SlotStatus.State.valueOf(state.state().toString()));
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
