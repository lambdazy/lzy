package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

import java.net.URI;
import java.util.Base64;

public class Snapshot implements LzyCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2)
            throw new IllegalArgumentException("Please specify snapshot command");
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                .build();
        final LzyKharonGrpc.LzyKharonBlockingStub server = LzyKharonGrpc.newBlockingStub(serverCh);
        switch (command.getArgs()[1]) {
            case "create": {
                final LzyWhiteboard.Snapshot snapshotId = server.createSnapshot(LzyWhiteboard.CreateSnapshotCommand
                        .newBuilder()
                        .setAuth(auth)
                        .build()
                );
                System.out.println(JsonFormat.printer().print(snapshotId));
                break;
            }
            case "finalize": {
                final LzyWhiteboard.OperationStatus operationStatus = server.finalizeSnapshot(LzyWhiteboard.FinalizeSnapshotCommand
                        .newBuilder()
                        .setSnapshotId(command.getArgs()[2])
                        .setAuth(auth)
                        .build()
                );
                System.out.println(JsonFormat.printer().print(operationStatus));
                break;
            }
        }
        return 0;
    }
}

