package ai.lzy.servant.commands;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyServantGrpc;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;

import java.util.Base64;

public class Update implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final ManagedChannel channel = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub terminal = LzyServantGrpc.newBlockingStub(channel);
        final LzyAuth.Auth auth = LzyAuth.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        //noinspection ResultOfMethodCallIgnored
        terminal.update(auth);
        return 0;
    }
}
