package ai.lzy.servant.commands;

import io.grpc.ManagedChannel;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyServantGrpc;

public class Update implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final ManagedChannel channel = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub terminal = LzyServantGrpc.newBlockingStub(channel);
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        //noinspection ResultOfMethodCallIgnored
        terminal.update(auth);
        return 0;
    }
}
