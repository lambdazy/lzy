package ai.lzy.servant.commands;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.time.Instant;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;

public class Snapshot implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify snapshot command");
        }
        final IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }
        final URI serverAddr = URI.create(command.getOptionValue('w'));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
            .build();
        final SnapshotApiGrpc.SnapshotApiBlockingStub server = SnapshotApiGrpc.newBlockingStub(serverCh);
        switch (command.getArgs()[1]) {
            case "create": {
                Instant time = Instant.now();
                Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond()).build();
                final LzyWhiteboard.Snapshot snapshotId = server
                    .createSnapshot(LzyWhiteboard.CreateSnapshotCommand
                        .newBuilder()
                        .setAuth(auth)
                        .setCreationDateUTC(timestamp)
                        .setWorkflowName(command.getArgs()[2])
                        .build()
                    );
                System.out.println(JsonFormat.printer().print(snapshotId));
                break;
            }
            case "finalize": {
                final LzyWhiteboard.OperationStatus operationStatus = server
                    .finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand
                        .newBuilder()
                        .setSnapshotId(command.getArgs()[2])
                        .setAuth(auth)
                        .build()
                    );
                System.out.println(JsonFormat.printer().print(operationStatus));
                break;
            }
            case "last": {
                LzyWhiteboard.Snapshot snapshot = server.lastSnapshot(
                    LzyWhiteboard.LastSnapshotCommand.newBuilder()
                        .setAuth(auth)
                        .setWorkflowName(command.getArgs()[2])
                        .build()
                );
                System.out.println(JsonFormat.printer().print(snapshot));
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + command.getArgs()[1]);
        }
        return 0;
    }
}

