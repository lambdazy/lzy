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
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.commands.Channel;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class LzyServant {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);

    private static final Options options = new Options();
    static {
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(new Option("a", "auth", true, "Enforce auth"));
        options.addOption(new Option("z", "lzy-address", true, "Lzy server address [host:port]"));
        options.addOption(new Option("m", "lzy-mount", true, "Lzy FS mount point"));
        options.addOption(new Option("h", "host", true, "Servant host name"));
        options.addOption(new Option("k", "private-key", true, "Path to private key for user auth"));
        Channel.populateOptions(options);
    }

    public static void main(String[] args) throws Exception {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            final CommandLine parse = cliParser.parse(options, args);
            if (parse.getArgs().length > 0) {
                final ServantCommand.Commands command = ServantCommand.Commands.valueOf(parse.getArgs()[0]);
                System.exit(command.execute(parse));
            }
            else new Start().execute(parse);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("lzy-servant", options);
            System.exit(-1);
        }
    }

    public static class Builder {
        private URI serverAddr;
        private String servantName;
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
                authBuilder.setUser(IAM.UserCredentials.newBuilder()
                    .setUserId(user)
                    .setToken(token)
                    .setTokenSign(tokenSign)
                    .build());
            }
            else {
                authBuilder.setTask(IAM.TaskCredentials.newBuilder()
                    .setTaskId(task)
                    .setToken(token)
                    .build()
                );
            }
            final URI servantAddress = new URI(null, null, servantName, servantPort, null, null, null);
            final Impl impl = new Impl(root, servantAddress, serverAddr, authBuilder.build());
            final Server server = ServerBuilder.forPort(servantPort).addService(impl).build();
            Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
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

        public Builder servantPort(int port) {
            this.servantPort = port;
            return this;
        }
    }

    private final Server server;
    private final Impl impl;

    public LzyServant(Server server, Impl impl) {
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

    private static class Impl extends LzyServantGrpc.LzyServantImplBase implements AutoCloseable {
        private final LzyServerGrpc.LzyServerBlockingStub server;
        private final URI serverAddress;
        private final IAM.Auth auth;
        private Execution currentExecution;
        private final LzyFS lzyFS;
        private final URI servantAddress;

        private Impl(Path mount, URI servantAddress, URI serverAddress, IAM.Auth auth) {
            this.auth = auth;
            this.servantAddress = servantAddress;
            this.serverAddress = serverAddress;
            this.lzyFS = new LzyFS();
            this.lzyFS.mount(mount);
            final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(serverAddress.getHost(), serverAddress.getPort())
                .usePlaintext()
                .build();
            this.server = LzyServerGrpc.newBlockingStub(channel);
        }

        @Override
        public void close() {
            lzyFS.umount();
        }

        void register() {
            final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
            commandBuilder.setAuth(auth);
            commandBuilder.setServantURI(servantAddress.toString());
            //noinspection ResultOfMethodCallIgnored
            server.registerServant(commandBuilder.build());
            for (ServantCommand.Commands command : ServantCommand.Commands.values()) {
                publishTool(null, Paths.get(command.name()), command.name());
            }
            final Operations.ZygoteList zygotes = server.zygotes(auth);
            for (Operations.RegisteredZygote zygote : zygotes.getZygoteList()) {
                publishTool(
                    gRPCConverter.from(zygote.getWorkload()),
                    Paths.get(zygote.getName()),
                    "run", zygote.getName()
                );
            }
        }

        private void publishTool(Zygote z, Path to, String... servantArgs) {
            final List<String> commandParts = new ArrayList<>();
            commandParts.add(System.getProperty("java.home") + "/bin/java");
            commandParts.add("-Xmx1g");
            commandParts.add("-classpath");
            commandParts.add('"' + System.getProperty("java.class.path") + '"');
            commandParts.add(LzyServant.class.getCanonicalName());
            commandParts.addAll(Arrays.asList(servantArgs));
            commandParts.addAll(List.of("--port", Integer.toString(servantAddress.getPort())));
            commandParts.addAll(List.of("--lzy-address", serverAddress.toString()));
            commandParts.addAll(List.of("--auth", new String(Base64.getEncoder().encode(auth.toByteString().toByteArray()))));
            commandParts.add("$@");

            final String script = String.join(" ", commandParts) + "\n";
            lzyFS.addScript(new LzyScript() {
                @Override
                public Zygote operation() {
                    return z;
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

        @Override
        public void execute(Servant.ExecutionSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            try {
                LOG.info("Starting execution " + JsonFormat.printer().print(request));
            }
            catch (InvalidProtocolBufferException ignore) {}
            if (currentExecution != null) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.asException());
                return;
            }
            final String tid = request.getTaskId();
            final AtomicZygote from = request.hasDefinition() ? (AtomicZygote) gRPCConverter.from(request.getDefinition()): null;
            this.currentExecution = new Execution(tid, from);
            this.currentExecution.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.hasExit()) {
                    this.currentExecution = null;
                    responseObserver.onCompleted();
                }
            });
            Context.current().addListener(context -> {
                LOG.info("Execution terminated from server ");
                System.exit(1);
            }, Runnable::run);
            if (request.hasDefinition())
                this.currentExecution.start();
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            if (currentExecution == null || currentExecution.slot(request.getSlot()) != null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            final LzyOutputSlot slot = (LzyOutputSlot)currentExecution.slot(request.getSlot());
            Context.current().addListener(context -> slot.state(Operations.SlotStatus.State.SUSPENDED), Runnable::run);
            slot.state(Operations.SlotStatus.State.OPEN);
            try {
                slot.readFromPosition(request.getOffset())
                    .forEach(chunk -> responseObserver.onNext(Servant.Message.newBuilder().setChunk(chunk).build()));
                responseObserver.onCompleted();
            }
            catch (IOException iae) {
                responseObserver.onError(iae);
            }
        }

        @Override
        public void configureSlot(Servant.SlotCommand request, StreamObserver<Servant.SlotCommandStatus> responseObserver) {
            if (currentExecution == null || currentExecution.slot(request.getSlot()) != null) {
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
                    final LzySlot lzySlot = currentExecution.configureSlot(
                        gRPCConverter.from(create.getSlot()),
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
                    responseObserver.onNext(Servant.SlotCommandStatus.newBuilder().setStatus(slot.status()).build());
                    responseObserver.onCompleted();
                    return;
                case CLOSE:
                    ((LzyInputSlot) slot).close();
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
                publishTool(
                    gRPCConverter.from(zygote.getWorkload()),
                    Paths.get(zygote.getName()),
                    "run", zygote.getName()
                );
            }
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}
