package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

public class Cash implements LzyCommand {

    private static final Options options = new Options();

    @Override
    public int execute(CommandLine command) throws Exception {
        IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify operation type");
        }

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
            .build();
        final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot = SnapshotApiGrpc.newBlockingStub(serverCh);

        switch (command.getArgs()[1]) {
            case "save": {
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Please specify execution spec file");
                }
                LzyWhiteboard.SaveExecutionCommand.Builder builder = LzyWhiteboard.SaveExecutionCommand
                    .newBuilder();

                JsonFormat.parser()
                    .merge(Files.newBufferedReader(Paths.get(command.getArgs()[2])), builder);

                LzyWhiteboard.SaveExecutionResponse resp = snapshot.saveOperation(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            case "find": {
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Please specify execution spec file");
                }
                LzyWhiteboard.ResolveExecutionCommand.Builder builder = LzyWhiteboard.ResolveExecutionCommand
                    .newBuilder();

                JsonFormat.parser()
                    .merge(Files.newBufferedReader(Paths.get(command.getArgs()[2])), builder);

                LzyWhiteboard.ResolveExecutionResponse resp = snapshot.resolveOperation(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            default: {
                System.out.println("Wrong credentials type");
                return -1;
            }
        }
    }
}
