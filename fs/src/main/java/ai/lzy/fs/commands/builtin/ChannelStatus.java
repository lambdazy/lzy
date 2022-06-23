package ai.lzy.fs.commands.builtin;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.net.URI;
import java.util.Base64;

public final class ChannelStatus implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final URI serverAddr = URI.create("grpc://" + command.getOptionValue('z'));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();

        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);
        final Channels.ChannelStatusList statusList = server.channelsStatus(auth);

        for (final Channels.ChannelStatus status : statusList.getStatusesList()) {
            System.out.println(JsonFormat.printer().print(status));
        }

        return 0;
    }

}
