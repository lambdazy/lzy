package ai.lzy.fs.commands.builtin;

import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.LzyChannelManagerGrpc;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.IAM;

import java.net.URI;
import java.util.Base64;

public final class ChannelStatus implements LzyCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        final URI channelManagerAddress = URI.create("grpc://" + command.getOptionValue("channel-manager"));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        final ManagedChannel channelManagerChannel = ChannelBuilder
            .forAddress(channelManagerAddress.getHost(), channelManagerAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyChannelManagerGrpc.SERVICE_NAME)
            .build();

        final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager =
            LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel)
                .withInterceptors(ClientHeaderInterceptor.header(
                    GrpcHeaders.AUTHORIZATION,
                    auth.getUser()::getToken
                ));

        final ChannelManager.ChannelStatusList channelStatusList = channelManager.channelsStatus(
            ChannelManager.ChannelsStatusRequest.newBuilder().build());

        for (var status : channelStatusList.getStatusesList()) {
            System.out.println(JsonFormat.printer().print(status));
        }

        return 0;
    }

}
