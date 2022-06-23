package ai.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v2.IAM;
import ai.lzy.priv.v2.Lzy;
import ai.lzy.priv.v2.LzyKharonGrpc;
import ai.lzy.priv.v2.LzyServerGrpc;

public class Sessions implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);

        Lzy.GetSessionsRequest.Builder builder = Lzy.GetSessionsRequest
            .newBuilder()
            .setAuth(auth.getUser());

        Lzy.GetSessionsResponse resp = server.getSessions(builder.build());
        System.out.println(JsonFormat.printer().print(resp));
        return 0;
    }
}
