package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyWhiteboard;

import java.net.URI;
import java.util.Base64;

public class Whiteboard implements LzyCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2)
            throw new IllegalArgumentException("Please specify whiteboard command");
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ManagedChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .build();
        final LzyKharonGrpc.LzyKharonBlockingStub server = LzyKharonGrpc.newBlockingStub(serverCh);
        switch (command.getArgs()[1]) {
            case "getWhiteboard": {
                final LzyWhiteboard.Whiteboard whiteboard = server.getWhiteboard(LzyWhiteboard.GetWhiteboardCommand
                        .newBuilder()
                        .setWbId(command.getArgs()[2])
                        .setAuth(auth.getUser())
                        .build()
                );
                System.out.println(JsonFormat.printer().print(whiteboard));
                break;
            }
            case "getId": {
                final LzyWhiteboard.WhiteboardId whiteboard = server.getWhiteboardId(LzyWhiteboard.GetWhiteboardIdCommand
                        .newBuilder()
                        .setCustomId(command.getArgs()[2])
                        .setUserCredentials(auth.getUser())
                        .build()
                );
                System.out.println(JsonFormat.printer().print(whiteboard));
                break;
            }
        }
        return 0;
    }
}
