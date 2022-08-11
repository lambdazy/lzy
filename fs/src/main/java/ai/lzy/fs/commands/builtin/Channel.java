package ai.lzy.fs.commands.builtin;

import static ai.lzy.model.GrpcConverter.to;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.v1.ChannelManager;
import ai.lzy.v1.Channels;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyChannelManagerGrpc;
import ai.lzy.v1.LzyKharonGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Channel implements LzyCommand {

    private static final Logger LOG = LogManager.getLogger(Channel.class);
    private static final Options options = new Options();

    private final ObjectMapper objectMapper = new ObjectMapper();
    /*
     * ~$ channel <common-opts> channel-cmd channel-name -c content-type -t channel-type
     *                                                   -w workflow-id -s snapshot-id -e entry-id
     *
     *   argNo:                  #1          #2           <parsed>
     */

    static {
        options.addOption("c", "content-type", true, "Content type");
        options.addOption("t", "channel-type", true, "Channel type (direct or snapshot)");
        options.addOption("w", "workflow-id", true, "Workflow id");
        options.addOption("s", "snapshot-id", true, "Snapshot id. Must be set if channel type is `snapshot`");
        options.addOption("e", "entry-id", true, "Snapshot entry id. Must be set if channel type is `snapshot`");
    }

    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            //noinspection CheckStyle
            throw new IllegalArgumentException(
                "Invalid call format. Expected: "
                    + "channel <common-opts> cmd "
                    + "[name|channel-id] "
                    + "[-c content-type] "
                    + "[-t channel-type] "
                    + "[-w workflow-id] "
                    + "[-s snapshot-id] "
                    + "[-e entry-id]");
        }

        final CommandLine localCmd = parse(command, options);
        if (localCmd == null) {
            return -1;
        }

        final String channelCommand = command.getArgList().get(1);
        String channelName = command.getArgList().size() > 2 ? command.getArgList().get(2) : null;

        final URI channelManagerAddress = URI.create("grpc://" + command.getOptionValue("channel-manager"));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        LOG.info("Auth {}", JsonUtils.printRequest(auth));

        final ManagedChannel channelManagerChannel = ChannelBuilder
            .forAddress(channelManagerAddress.getHost(), channelManagerAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();

        final LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager =
            LzyChannelManagerGrpc
                .newBlockingStub(channelManagerChannel)
                .withInterceptors(ClientHeaderInterceptor.header(
                    GrpcHeaders.AUTHORIZATION,
                    auth.getUser()::getToken
                ));

        switch (channelCommand) {
            case "create" -> {
                if (channelName == null) {
                    channelName = UUID.randomUUID().toString();
                }

                DataSchema data;
                if (localCmd.hasOption('c')) {
                    final String mappingFile = localCmd.getOptionValue('c');
                    // TODO(aleksZubakov): drop this ugly stuff when already fully switched to grpc api
                    //noinspection unchecked
                    final Map<String, String> bindings = new HashMap<String, String>(
                        objectMapper.readValue(new File(mappingFile), Map.class));

                    String dataSchemeType = bindings.get("schemeType");
                    String contentType = bindings.getOrDefault("type", "");
                    LOG.info("building dataschema from args {} and {}", dataSchemeType, contentType);
                    data = DataSchema.buildDataSchema(dataSchemeType, contentType);
                } else {
                    data = DataSchema.plain;
                }

                final Channels.ChannelSpec.Builder channelSpecBuilder = Channels.ChannelSpec.newBuilder();
                channelSpecBuilder.setContentType(to(data));
                channelSpecBuilder.setChannelName(channelName);

                if ("snapshot".equals(localCmd.getOptionValue('t'))) {
                    channelSpecBuilder.setSnapshot(
                        Channels.SnapshotChannelType.newBuilder()
                            .setSnapshotId(localCmd.getOptionValue('s'))
                            .setEntryId(localCmd.getOptionValue('e'))
                            .build());
                } else {
                    channelSpecBuilder.setDirect(Channels.DirectChannelType.newBuilder().build());
                }

                String workflowId = command.getOptionValue('w');
                final ChannelManager.ChannelCreateResponse channelCreateResponse = channelManager.create(
                    ChannelManager.ChannelCreateRequest.newBuilder()
                        .setWorkflowId(workflowId)
                        .setChannelSpec(channelSpecBuilder.build())
                        .build());

                System.out.println(JsonUtils.printRequest(channelCreateResponse));
            }
            case "status" -> {
                if (channelName == null) {
                    throw new IllegalArgumentException("Specify a channel id");
                }

                try {
                    final ChannelManager.ChannelStatus channelStatus = channelManager.status(
                        ChannelManager.ChannelStatusRequest.newBuilder()
                            .setChannelId(channelName)
                            .build()
                    );
                    System.out.println(JsonFormat.printer().print(channelStatus));
                } catch (StatusRuntimeException e) {
                    System.out.println(
                        "Got exception while channel status (status_code=" + e.getStatus().getCode() + ")");
                    return -1;
                }
            }
            case "destroy" -> {
                if (channelName == null) {
                    throw new IllegalArgumentException("Specify a channel id");
                }

                try {
                    final ChannelManager.ChannelDestroyResponse destroyResponse = channelManager.destroy(
                        ChannelManager.ChannelDestroyRequest.newBuilder()
                            .setChannelId(channelName)
                            .build());
                    System.out.println(JsonFormat.printer().print(destroyResponse));
                    System.out.println("Channel destroyed");
                } catch (StatusRuntimeException e) {
                    System.out.println(
                        "Got exception while channel destroy (status_code=" + e.getStatus().getCode() + ")");
                    return -1;
                }
            }
            default -> throw new IllegalStateException("Unknown channel command: " + channelCommand);
        }

        return 0;
    }
}
