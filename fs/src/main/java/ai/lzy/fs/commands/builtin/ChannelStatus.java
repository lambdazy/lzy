package ai.lzy.fs.commands.builtin;

import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.LzyChannelManagerGrpc;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.Channels;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyServerGrpc;

import java.net.URI;
import java.util.Base64;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public final class ChannelStatus implements LzyCommand {
    private static final Options options = new Options();

    @Override
    public int execute(CommandLine command) throws Exception {
        final CommandLine localCmd = parse(command, options);
        if (localCmd == null) {
            return -1;
        }
        final URI channelManagerAddress = URI.create("grpc://" + localCmd.getOptionValue("channel-manager"));
        // FIXME(d-kruchinin): fix auth to new format
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        final ManagedChannel channelManagerChannel = ChannelBuilder
            .forAddress(channelManagerAddress.getHost(), channelManagerAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyChannelManagerGrpc.SERVICE_NAME)
            .build();

        final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager =
            LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel);

        final ChannelManager.ChannelStatusList channelStatusList = channelManager.channelsStatus(
            ChannelManager.ChannelsStatusRequest.newBuilder().build());

        for (var status : channelStatusList.getStatusesList()) {
            System.out.println(JsonFormat.printer().print(status));
        }

        return 0;
    }

}
