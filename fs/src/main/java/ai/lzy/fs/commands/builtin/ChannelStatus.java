package ai.lzy.fs.commands.builtin;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.deprecated.LzyAuth;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;

import java.net.URI;
import java.util.Base64;

public final class ChannelStatus implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final URI channelManagerAddress = URI.create("grpc://" + command.getOptionValue("channel-manager"));
        final LzyAuth.Auth auth = LzyAuth.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final String workflowId = command.getOptionValue('i');

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

        final LCMS.ChannelStatusList channelStatusList = channelManager.statusAll(
            LCMS.ChannelStatusAllRequest.newBuilder().setWorkflowId(workflowId).build());

        for (var status : channelStatusList.getStatusesList()) {
            System.out.println(JsonFormat.printer().print(status));
        }

        return 0;
    }

}
