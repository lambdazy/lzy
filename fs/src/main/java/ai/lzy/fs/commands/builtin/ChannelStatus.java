package ai.lzy.fs.commands.builtin;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyChannelManagerGrpc;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ChannelStatus implements LzyCommand {

    private static final Logger LOG = LogManager.getLogger(ChannelStatus.class);
    private static final Options options = new Options();

    static {
        options.addOption("w", "workflow-id", true, "Workflow id");
    }

    @Override
    public int execute(CommandLine command) throws Exception {
        final URI channelManagerAddress = URI.create("grpc://" + command.getOptionValue("channel-manager"));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final String workflowId = command.getOptionValue('w');

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

        final ChannelManager.ChannelStatusList channelStatusList = channelManager.statusAll(
            ChannelManager.ChannelStatusAllRequest.newBuilder().setWorkflowId(workflowId).build());

        for (var status : channelStatusList.getStatusesList()) {
            System.out.println(JsonFormat.printer().print(status));
        }

        return 0;
    }

}
