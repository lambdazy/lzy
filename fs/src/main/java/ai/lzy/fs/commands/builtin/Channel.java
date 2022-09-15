package ai.lzy.fs.commands.builtin;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.model.DataScheme;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.channel.LCMS;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Channel implements LzyCommand {

    private static final Logger LOG = LogManager.getLogger(Channel.class);
    private static final Options options = new Options();

    private final ObjectMapper objectMapper = new ObjectMapper();
    /*
     * ~$ channel <common-opts> channel-cmd channel-id -n channel-name -c content-type -t channel-type
     *                                                 -s snapshot-id -e entry-id
     *
     *   argNo:                  #1          #2         <parsed>
     */

    static {
        options.addOption("n", "channel-name", true, "Channel name");
        options.addOption("c", "content-type", true, "Content type");
        options.addOption("t", "channel-type", true, "Channel type (direct or snapshot)");
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
                    + "[channel-id] "
                    + "[-n channel-name] "
                    + "[-c content-type] "
                    + "[-t channel-type] "
                    + "[-s snapshot-id] "
                    + "[-e entry-id]");
        }

        final CommandLine localCmd = parse(command, options);
        if (localCmd == null) {
            return -1;
        }

        final String channelCommand = command.getArgList().get(1);
        String channelId = null;
        if (command.getArgList().size() > 2 && !command.getArgList().get(2).startsWith("-")) {
            channelId = command.getArgList().get(2);
        }

        final URI channelManagerAddress = URI.create("grpc://" + command.getOptionValue("channel-manager"));
        final LzyAuth.Auth auth = LzyAuth.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
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
                if (channelId != null) {
                    LOG.warn("Channel id {} won't be used, id will be generated by channel manager", channelId);
                }
                final String channelName = localCmd.getOptionValue('n');

                DataScheme data;
                if (localCmd.hasOption('c')) {
                    final String mappingFile = localCmd.getOptionValue('c');
                    // TODO(aleksZubakov): drop this ugly stuff when already fully switched to grpc api
                    //noinspection unchecked
                    final Map<String, String> bindings = new HashMap<String, String>(
                        objectMapper.readValue(new File(mappingFile), Map.class));

                    String dataFormat = bindings.get("dataFormat");
                    String schemeContent = bindings.getOrDefault("schemeContent", "default");
                    LOG.info("building dataschema from args {} and {}", dataFormat, schemeContent);
                    data = new DataScheme(dataFormat, "", schemeContent, Map.of());
                } else {
                    data = DataScheme.PLAIN;
                }

                final var channelSpecBuilder = ai.lzy.v1.channel.LCM.ChannelSpec.newBuilder();
                channelSpecBuilder.setContentType(ProtoConverter.toProto(data));
                channelSpecBuilder.setChannelName(channelName);

                if ("snapshot".equals(localCmd.getOptionValue('t'))) {
                    channelSpecBuilder.setSnapshot(
                        ai.lzy.v1.channel.LCM.SnapshotChannelType.newBuilder()
                            .setSnapshotId(localCmd.getOptionValue('s'))
                            .setEntryId(localCmd.getOptionValue('e'))
                            .build());
                } else {
                    channelSpecBuilder.setDirect(ai.lzy.v1.channel.LCM.DirectChannelType.newBuilder().build());
                }

                String workflowId = command.getOptionValue('i');
                final LCMS.ChannelCreateResponse channelCreateResponse = channelManager.create(
                    LCMS.ChannelCreateRequest.newBuilder()
                        .setWorkflowId(workflowId)
                        .setChannelSpec(channelSpecBuilder.build())
                        .build());

                System.out.println(JsonUtils.printRequest(channelCreateResponse));
            }
            case "status" -> {
                if (channelId == null) {
                    throw new IllegalArgumentException("Specify a channel id");
                }

                try {
                    final LCMS.ChannelStatus channelStatus = channelManager.status(
                        LCMS.ChannelStatusRequest.newBuilder()
                            .setChannelId(channelId)
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
                if (channelId == null) {
                    throw new IllegalArgumentException("Specify a channel id");
                }

                try {
                    final LCMS.ChannelDestroyResponse destroyResponse = channelManager.destroy(
                        LCMS.ChannelDestroyRequest.newBuilder()
                            .setChannelId(channelId)
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
