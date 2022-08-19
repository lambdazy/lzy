package ai.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyWhiteboard;
import ai.lzy.v1.SnapshotApiGrpc;

public class Cache implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify operation type");
        }

        final URI serverAddr = URI.create(command.getOptionValue('w'));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
            .build();
        final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot = SnapshotApiGrpc.newBlockingStub(serverCh);

        switch (command.getArgs()[1]) {
            case "save" -> {
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Please specify execution spec file");
                }
                LzyWhiteboard.SaveExecutionCommand.Builder builder = LzyWhiteboard.SaveExecutionCommand
                    .newBuilder();

                JsonFormat.parser()
                    .merge(Files.newBufferedReader(Paths.get(command.getArgs()[2])), builder);

                builder.setAuth(auth);

                LzyWhiteboard.SaveExecutionResponse resp = snapshot.saveExecution(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            case "find" -> {
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Please specify execution spec file");
                }
                LzyWhiteboard.ResolveExecutionCommand.Builder builder = LzyWhiteboard.ResolveExecutionCommand
                    .newBuilder();

                JsonFormat.parser()
                    .merge(Files.newBufferedReader(Paths.get(command.getArgs()[2])), builder);

                builder.setAuth(auth);

                LzyWhiteboard.ResolveExecutionResponse resp = snapshot.resolveExecution(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            default -> {
                System.out.println("Wrong command");
                return -1;
            }
        }
    }
}
