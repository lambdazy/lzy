package ru.yandex.cloud.ml.platform.lzy.servant;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
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
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Start;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyScript;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LzyServant {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);

    private static final Options options = new Options();
    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(new Option("a", "auth", true, "Enforce auth"));
        options.addOption(new Option("z", "lzy-address", true, "Lzy server address [host:port]"));
        options.addOption(new Option("m", "lzy-mount", true, "Lzy FS mount point"));
        options.addOption(new Option("h", "host", true, "Servant host name"));
        options.addOption(new Option("i", "internal-host", true, "Servant host name for connection from another servants"));
        options.addOption(new Option("k", "private-key", true, "Path to private key for user auth"));
    }

    public static void main(String[] args) throws Exception {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        String commandStr = "lzy-servant";
        try {
            final CommandLine parse = cliParser.parse(options, args, true);
            if (parse.getArgs().length > 0) {
                commandStr = parse.getArgs()[0];
                final ServantCommand.Commands command = ServantCommand.Commands.valueOf(commandStr);
                System.exit(command.execute(parse));
            }
            else new Start().execute(parse);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp(commandStr, options);
            System.exit(-1);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public static class Builder {
        private final URI serverAddr;
        private String servantName;
        private String servantInternalName;
        private String token;
        private Path root;
        private String tokenSign;
        private String user;
        private String task;
        private int servantPort = -1;

        public static Builder forLzyServer(URI serverAddr) {
            return new Builder(serverAddr);
        }

        private Builder(URI serverAddr) {
            this.serverAddr = serverAddr;
        }

        public LzyServant build() throws URISyntaxException {
            final IAM.Auth.Builder authBuilder = IAM.Auth.newBuilder();
            if (user != null) {
                final IAM.UserCredentials.Builder credBuilder = IAM.UserCredentials.newBuilder()
                    .setUserId(user)
                    .setToken(token);
                if (tokenSign != null) {
                    credBuilder.setTokenSign(tokenSign);
                }
                authBuilder.setUser(credBuilder.build());
            } else {
                authBuilder.setTask(IAM.TaskCredentials.newBuilder()
                    .setTaskId(task)
                    .setToken(token)
                    .build()
                );
            }
            try {
                Files.createDirectories(root);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final URI servantAddress = new URI("http", null, servantName, servantPort, null, null, null);
            final URI servantInternalAddress = servantInternalName == null ? servantAddress : new URI("http", null, servantInternalName, servantPort, null, null, null);
            final Impl impl = new Impl(root, servantAddress, servantInternalAddress, serverAddr, authBuilder.build());
            final Server server = ServerBuilder.forPort(servantPort).addService(impl).build();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.shutdown();
                impl.close();
            }));
            return new LzyServant(server, impl);
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder tokenSign(String tokenSign) {
            this.tokenSign = tokenSign;
            return this;
        }

        public Builder root(Path root) {
            this.root = root;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder task(String task) {
            this.task = task;
            return this;
        }

        public Builder servantName(String servantName) {
            this.servantName = servantName;
            return this;
        }

        public Builder servantInternalName(String servantInternalName) {
            this.servantInternalName = servantInternalName;
            return this;
        }

        public Builder servantPort(int port) {
            this.servantPort = port;
            return this;
        }
    }

    private final Server server;
    private final Impl impl;

    private LzyServant(Server server, Impl impl) {
        this.server = server;
        this.impl = impl;
    }

    public void start() throws IOException {
        server.start();
        impl.register();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    private static class Impl extends LzyServantGrpc.LzyServantImplBase {
        private final LzyServerGrpc.LzyServerBlockingStub server;
        private final URI serverAddress;
        private final Path mount;
        private final IAM.Auth auth;
        private LzyExecution currentExecution;
        private final LzyFS lzyFS;
        private final URI servantAddress;
        private final URI servantInternalAddress;
        private final AtomicReference<ServantStatus> status = new AtomicReference<>(ServantStatus.STARTED);

        private Impl(Path mount, URI servantAddress, URI servantInternalAddress, URI serverAddress, IAM.Auth auth) {
            this.mount = mount;
            this.servantInternalAddress = servantInternalAddress;
            this.auth = auth;
            this.servantAddress = servantAddress;
            this.serverAddress = serverAddress;
            this.lzyFS = new LzyFS();
            this.lzyFS.mount(mount, false, false);
            //this.lzyFS.mount(mount, false, true);
            final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(serverAddress.getHost(), serverAddress.getPort())
                .usePlaintext()
                .build();
            this.server = LzyServerGrpc.newBlockingStub(channel);
        }

        public void close() {
            lzyFS.umount();
        }

        void register() {
            status.set(ServantStatus.REGISTERING);
            for (ServantCommand.Commands command : ServantCommand.Commands.values()) {
                publishTool(null, Paths.get(command.name()), command.name());
            }
            final Operations.ZygoteList zygotes = server.zygotes(auth);
            for (Operations.RegisteredZygote zygote : zygotes.getZygoteList()) {
                publishTool(
                    zygote.getWorkload(),
                    Paths.get(zygote.getName()),
                    "run"
                );
            }
            LOG.info("Registering servant " + servantAddress + " at " + serverAddress);
            final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
            commandBuilder.setAuth(auth);
            commandBuilder.setServantURI(servantAddress.toString());
            //noinspection ResultOfMethodCallIgnored
            server.registerServant(commandBuilder.build());
            status.set(ServantStatus.REGISTERED);
        }

        private void publishTool(Operations.Zygote z, Path to, String... servantArgs) {
            try {
                final String zygoteJson = z != null ? JsonFormat.printer().print(z) : null;
                final String logConfFile = System.getProperty("log4j.configurationFile");
                final List<String> commandParts = new ArrayList<>();
                commandParts.add(System.getProperty("java.home") + "/bin/java");
                commandParts.add("-Xmx1g");
                commandParts.add("-Dcustom.log.file=" + to.getFileName() + "_$(($RANDOM % 10000))");
                if (logConfFile != null) {
                    commandParts.add("-Dlog4j.configurationFile=" + logConfFile);
                }
                commandParts.add("-classpath");
                commandParts.add('"' + System.getProperty("java.class.path") + '"');
                commandParts.add(LzyServant.class.getCanonicalName());
                commandParts.addAll(List.of("--port", Integer.toString(servantAddress.getPort())));
                commandParts.addAll(List.of("--lzy-address", serverAddress.toString()));
                commandParts.addAll(List.of("--lzy-mount", mount.toAbsolutePath().toString()));
                commandParts.addAll(List.of("--auth", new String(Base64.getEncoder().encode(auth.toByteString().toByteArray()))));
                commandParts.addAll(Arrays.asList(servantArgs));
                commandParts.add("$@");

                final StringBuilder scriptBuilder = new StringBuilder();
                if (zygoteJson != null) {
                    scriptBuilder.append("export ZYGOTE=")
                        .append('"')
                        .append(zygoteJson
                            .replaceAll("\"", "\\\\\"")
                            .replaceAll("\\R", "\\\\\n")
                        )
                        .append('"')
                        .append("\n\n");
                }
                scriptBuilder.append(String.join(" ", commandParts)).append("\n");
                final String script = scriptBuilder.toString();
                lzyFS.addScript(new LzyScript() {
                    @Override
                    public Zygote operation() {
                        return gRPCConverter.from(z);
                    }

                    @Override
                    public Path location() {
                        return to;
                    }

                    @Override
                    public CharSequence scriptText() {
                        return script;
                    }
                }, z == null);

            }
            catch (InvalidProtocolBufferException ignore) {
            }
        }

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            status.set(ServantStatus.PREPARING_EXECUTION);
            LOG.info("Servant::execute " + JsonUtils.printRequest(request));
            if (currentExecution != null) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.asException());
                return;
            }
            if (request.hasZygote()) {
                final String tid = request.getAuth().getTask().getTaskId();
                this.currentExecution = new LzyExecution(tid, (AtomicZygote) gRPCConverter.from(request.getZygote()), servantInternalAddress);
            }
            else { // terminal
                this.currentExecution = new LzyExecution(null, null, servantInternalAddress);
            }
            this.currentExecution.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.hasDetach()) {
                    lzyFS.removeSlot(progress.getDetach().getSlot().getName());
                }
                if (progress.hasExit()) {
                    this.currentExecution = null;
                    responseObserver.onCompleted();
                }
            });
            Context.current().addListener(context -> {
                if (currentExecution != null) {
                    LOG.info("Execution terminated from server ");
                    System.exit(1);
                }
            }, Runnable::run);

            for (Tasks.SlotAssignment spec : request.getAssignmentsList()) {
                final LzySlot lzySlot = currentExecution.configureSlot(
                    gRPCConverter.from(spec.getSlot()),
                    spec.getBinding()
                );
                if (lzySlot instanceof LzyFileSlot) {
                    lzyFS.addSlot((LzyFileSlot) lzySlot);
                }
            }

            if (request.hasZygote())
                this.currentExecution.start();
            status.set(ServantStatus.EXECUTING);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            LOG.info("LzyServant::openOutputSlot " + JsonUtils.printRequest(request));
            if (currentExecution == null || currentExecution.slot(request.getSlot()) == null) {
                LOG.info("Not found slot: " + request.getSlot());
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            final LzyOutputSlot slot = (LzyOutputSlot)currentExecution.slot(request.getSlot());
            try {
                slot.readFromPosition(request.getOffset())
                    .forEach(chunk -> responseObserver.onNext(Servant.Message.newBuilder().setChunk(chunk).build()));
                responseObserver.onNext(Servant.Message.newBuilder().setControl(Servant.Message.Controls.EOS).build());
                responseObserver.onCompleted();
            }
            catch (IOException iae) {
                responseObserver.onError(iae);
            }
        }

        @Override
        public void configureSlot(Servant.SlotCommand request, StreamObserver<Servant.SlotCommandStatus> responseObserver) {
            LOG.info("Servant::configureSlot " + JsonUtils.printRequest(request));
            if (currentExecution == null) {
                LOG.error("Servant::configureSlot NOT_FOUND");
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            final LzySlot slot = currentExecution.slot(request.getSlot()); // null for create
            if (slot == null && request.getCommandCase() != Servant.SlotCommand.CommandCase.CREATE) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            switch (request.getCommandCase()) {
                case CREATE:
                    final Servant.CreateSlotCommand create = request.getCreate();
                    final Slot slotSpec = gRPCConverter.from(create.getSlot());
                    final LzySlot lzySlot = currentExecution.configureSlot(
                        slotSpec,
                        create.getChannelId()
                    );
                    if (lzySlot instanceof LzyFileSlot)
                        lzyFS.addSlot((LzyFileSlot)lzySlot);
                    break;
                case CONNECT:
                    final Servant.ConnectSlotCommand connect = request.getConnect();
                    ((LzyInputSlot) slot).connect(URI.create(connect.getSlotUri()));
                    break;
                case DISCONNECT:
                    ((LzyInputSlot) slot).disconnect();
                    break;
                case STATUS:
                    final Operations.SlotStatus.Builder status = Operations.SlotStatus.newBuilder(slot.status());
                    if (auth.hasUser()) {
                        status.setUser(auth.getUser().getUserId());
                    }
                    responseObserver.onNext(Servant.SlotCommandStatus.newBuilder().setStatus(status.build()).build());
                    responseObserver.onCompleted();
                    return;
                case CLOSE:
                    slot.close();
                    LOG.info("Explicitly closing slot " + slot.name());
                    break;
                default:
                    responseObserver.onError(Status.INVALID_ARGUMENT.asException());
                    return;
            }
            responseObserver.onNext(Servant.SlotCommandStatus.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void signal(Tasks.TaskSignal request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            if (currentExecution == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            currentExecution.signal(request.getSigValue());
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            final Operations.ZygoteList zygotes = server.zygotes(auth);
            for (Operations.RegisteredZygote zygote : zygotes.getZygoteList()) {
                publishTool(zygote.getWorkload(), Paths.get(zygote.getName()), "run", zygote.getName());
            }
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            final Servant.ServantStatus.Builder builder = Servant.ServantStatus.newBuilder();
            builder.setStatus(status.get().toGrpcServantStatus());
            if (currentExecution != null) {
                builder.addAllConnections(currentExecution.slots().map(slot -> {
                    final Operations.SlotStatus.Builder status = Operations.SlotStatus.newBuilder(slot.status());
                    if (auth.hasUser()) {
                        status.setUser(auth.getUser().getUserId());
                    }
                    return status.build();
                }).collect(Collectors.toList()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            LOG.info("Servant::stop");
            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
            System.exit(0);
        }
    }
}
