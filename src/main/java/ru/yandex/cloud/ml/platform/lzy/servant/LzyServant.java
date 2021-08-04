package ru.yandex.cloud.ml.platform.lzy.servant;

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
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFS;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
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
import java.nio.file.Path;

public class LzyServant {
    private static final Options options = new Options();
    static {
        options.addOption(new Option("c", "command", true, "Command to run, default is to run servant"));
        options.addOption(new Option("p", "port", true, "gRPC port setting"));
        options.addOption(new Option("t", "task-spec-file", true, "Task spec JSON file. If no zygote specified servant is run in user mode."));
        options.addOption(new Option("z", "lzy-server", true, "Lzy server address [host:port]"));
        options.addOption(new Option("m", "lzy-mount", true, "Lzy FS mount point"));
        options.addOption(new Option("a", "servant-address", true, "Servant address"));
    }

    public static void main(String[] args) throws InterruptedException {
        final CommandLineParser cliParser = new DefaultParser();
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            final CommandLine parse = cliParser.parse(options, args);
            if (parse.getArgs().length > 0) {
                switch (parse.getArgs()[0]) {
                    case "touch":
                        break;
                    case "create-channel":
                        break;

                }
            }
            else {
                startServant(parse);
            }
        } catch (ParseException | IOException e) {
            System.out.println(e.getMessage());
            cliHelp.printHelp("lzy-servant", options);
            System.exit(-1);
        }
    }

    private static void startServant(CommandLine parse) throws IOException, InterruptedException {
        final int port = Integer.parseInt(parse.getOptionValue('p', "9999"));
        final String lzyServerAddr = parse.getOptionValue('l', "localhost:8888");
        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress(
                lzyServerAddr.contains(":") ? lzyServerAddr.substring(0, lzyServerAddr.indexOf(":")) : lzyServerAddr,
                lzyServerAddr.contains(":") ? Integer.parseInt(lzyServerAddr.substring(lzyServerAddr.indexOf(":"))) : 8888
            )
            .build();
        final LzyServerGrpc.LzyServerBlockingStub lzyServer = LzyServerGrpc.newBlockingStub(channel);

        final Impl servant = new Impl(
            lzyServer,
            Path.of(parse.getOptionValue('m', "./lzy")),
            URI.create(parse.getOptionValue('a'))
        );
        final Server server = ServerBuilder.forPort(port).addService(servant).build();
        server.start();
        servant.register();
        server.awaitTermination();
    }

    private static class Impl extends LzyServantGrpc.LzyServantImplBase {
        private final LzyServerGrpc.LzyServerBlockingStub server;
        private Execution currentExecution;
        private final LzyFS lzyFS;
        private final URI servantAddress;
        private final String token;

        private Impl(LzyServerGrpc.LzyServerBlockingStub server, Path mount, URI servantAddress) {
            this.servantAddress = servantAddress;
            token = System.getenv("LZYTOKEN");
            this.server = server;
            this.lzyFS = new LzyFS();
            this.lzyFS.mount(mount);
        }

        void register() {
            final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant
                .newBuilder();
            commandBuilder.getAuthBuilder()
                .getUserBuilder()
                .setUserId(System.getenv("LZYUSER"))
                .setToken(token)
                .build();

            commandBuilder.setServantURI(servantAddress.toString());
            //noinspection ResultOfMethodCallIgnored
            server.registerServant(commandBuilder.build());
        }

        @Override
        public void execute(Servant.ExecutionSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            if (currentExecution != null) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.asException());
                return;
            }
            final String tid = request.getTaskId();
            this.currentExecution = new Execution(tid, (AtomicZygote)gRPCConverter.from(request.getDefinition()));
            this.currentExecution.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.hasExit())
                    this.currentExecution = null;
            });
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
                    ((LzyInputSlot) slot).connect(URI.create(connect.getServant()), connect.getSlot());
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
        public void signal(Tasks.SignalRequest request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            if (currentExecution == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            currentExecution.signal(request.getSigValue());
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}
