package ai.lzy.fs.commands.builtin;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.v1.channel.LCMPS;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.deprecated.LzyAuth;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;

import java.net.URI;
import java.util.Base64;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public final class ChannelStatus implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final URI channelManagerAddress = URI.create("grpc://" + command.getOptionValue("channel-manager"));
        final LzyAuth.Auth auth = LzyAuth.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final String workflowId = command.getOptionValue('i');

        final ManagedChannel channelManagerChannel = newGrpcChannel(
            channelManagerAddress.getHost(), channelManagerAddress.getPort(), LzyChannelManagerGrpc.SERVICE_NAME);

        final LzyChannelManagerPrivateGrpc.LzyChannelManagerPrivateBlockingStub channelManager = newBlockingClient(
            LzyChannelManagerPrivateGrpc.newBlockingStub(channelManagerChannel), "CmdCs", auth.getUser()::getToken);

        final LCMPS.ChannelStatusList channelStatusList = channelManager.statusAll(
            LCMPS.ChannelStatusAllRequest.newBuilder().setExecutionId(workflowId).build());

        for (var status : channelStatusList.getStatusesList()) {
            System.out.println(JsonFormat.printer().print(status));
        }

        return 0;
    }

}
