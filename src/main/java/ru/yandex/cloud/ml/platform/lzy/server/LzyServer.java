package ru.yandex.cloud.ml.platform.lzy.server;

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
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Channel;
import ru.yandex.cloud.ml.platform.lzy.server.local.LocalTasksManager;
import ru.yandex.cloud.ml.platform.lzy.server.mem.SimpleInMemAuthenticator;
import ru.yandex.cloud.ml.platform.lzy.server.mem.ZygoteRepositoryImpl;
import ru.yandex.cloud.ml.platform.lzy.server.task.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import ru.yandex.cloud.ml.platform.lzy.server.task.TaskException;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class LzyServer {
    private static final Options options = new Options();
    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        final Option source = new Option("s", "source", true, "Source directory to publish");
        source.setRequired(true);
        options.addOption(source);
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
        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));

        final Server server = ServerBuilder.forPort(port).addService(new Impl()).build();

        server.start();
        server.awaitTermination();
    }

    public static class Impl extends LzyServerGrpc.LzyServerImplBase {
        private final ZygoteRepository operations = new ZygoteRepositoryImpl();
        private final TasksManager tasks = new LocalTasksManager();
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
                .mergeFrom(operation)
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
        public void start(Tasks.TaskSpec request, StreamObserver<Tasks.TaskStatus> responseObserver) {
            if (!checkAuth(request.getAuth(), responseObserver))
                return;

            final Zygote workload = operations.get(request.getZygote());
            final Map<String, Channel> assignments = new HashMap<>();
            request.getAssignmentsList().forEach(ass ->
                assignments.put(ass.getSlotName(), tasks.channel(UUID.fromString(ass.getChannelId())))
            );

            final String uid = resolveUser(request.getAuth());
            final Task task = resolveTask(request.getAuth());
            final Tasks.TaskStatus status = getTaskStatus(() -> tasks.start(uid, task, workload, assignments, auth));

            responseObserver.onNext(status);
            responseObserver.onCompleted();
        }

        @Override
        public void signal(Tasks.SignalRequest request, StreamObserver<Tasks.TaskStatus> responseObserver) {
            if (!checkAuth(request.getAuth(), responseObserver))
                return;

            final Task task = tasks.task(UUID.fromString(request.getTid()));
            if (task == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            if (!auth.canAccess(task, resolveUser(request.getAuth()))) {
                responseObserver.onError(Status.PERMISSION_DENIED.asException());
                return;
            }
            if (request.getSig() == Tasks.SignalRequest.SIGType.NONE) {
                responseObserver.onNext(getTaskStatus(() -> task));
            }
            else {
                responseObserver.onNext(getTaskStatus(() -> {
                    task.signal(TasksManager.Signal.valueOf(request.getSigValue()));
                    return task;
                }));
            }
            responseObserver.onCompleted();
        }

        @Override
        public void processStatus(IAM.Auth auth, StreamObserver<Tasks.TasksList> responseObserver) {
            if (!checkAuth(auth, responseObserver))
                return;

            final String user = resolveUser(auth);
            final Tasks.TasksList.Builder builder = Tasks.TasksList.newBuilder();
            tasks.ps()
                .filter(t -> this.auth.canAccess(t, user))
                .map(t -> getTaskStatus(() -> t)).forEach(builder::addTasks);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void createChannel(Channels.ChannelRequest request, StreamObserver<Channels.Channel> responseObserver) {
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver))
                return;

            if (auth.hasUser()) { // user channel
                final Channel channel = tasks.createChannel(
                    resolveUser(auth),
                    resolveTask(auth),
                    gRPCConverter.contentTypeFrom(request.getContentType())
                );
                if (channel == null) {
                    responseObserver.onError(Status.ALREADY_EXISTS.asException());
                    return;
                }
                responseObserver.onNext(gRPCConverter.to(channel));
                responseObserver.onCompleted();
            }
        }

        @Override
        public void channelStatus(IAM.Auth auth, StreamObserver<Channels.ChannelStatusList> responseObserver) {
            if (!checkAuth(auth, responseObserver))
                return;

            final Channels.ChannelStatusList.Builder builder = Channels.ChannelStatusList.newBuilder();
            tasks.cs().forEach(channel -> {
                final Channels.ChannelStatus.Builder slotStatus = builder.addStatusesBuilder();
                slotStatus.setChannel(gRPCConverter.to(channel));
                for (SlotStatus state : tasks.connected(channel)) {
                    slotStatus.addConnectedBuilder()
                        .setTaskId(state.task().tid().toString())
                        .setConnectedTo(channel.id().toString())
                        .setDeclaration(gRPCConverter.to(state.slot()))
                        .setPointer(state.pointer())
                        .setState(Operations.SlotStatus.State.valueOf(state.state().toString()));
                }
            });
        }

        @Override
        public void registerServant(Lzy.AttachServant request, StreamObserver<Lzy.AttachStatus> responseObserver) {
            final IAM.Auth auth = request.getAuth();
            if (!checkAuth(auth, responseObserver))
                return;
            if (auth.hasTask()) {
                final Task task = tasks.task(UUID.fromString(auth.getTask().getTaskId()));
                task.attachServant(URI.create(request.getServantURI()));
            }
            responseObserver.onNext(Lzy.AttachStatus.newBuilder().build());
            responseObserver.onCompleted();
        }

        interface TaskProvider {
            Task get() throws TaskException;
        }

        private Tasks.TaskStatus getTaskStatus(TaskProvider supplier) {
            final Tasks.TaskStatus.Builder builder = Tasks.TaskStatus.newBuilder();
            try {
                final Task task = supplier.get();
                builder.setTaskId(task.tid().toString());

                builder.setStatus(Tasks.TaskStatus.Status.valueOf(task.state().toString()));
                builder.setServant(task.servant().toString());
                builder.setOwner(tasks.owner(task.tid()));
                Stream.concat(Stream.of(task.workload().input()), Stream.of(task.workload().output()))
                    .map(Slot::name)
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
