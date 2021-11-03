package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;

import java.util.Base64;

public class Update implements LzyCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .build();
        final LzyServantGrpc.LzyServantBlockingStub terminal = LzyServantGrpc.newBlockingStub(channel);
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        //noinspection ResultOfMethodCallIgnored
        terminal.update(auth);
        return 0;
    }
}
