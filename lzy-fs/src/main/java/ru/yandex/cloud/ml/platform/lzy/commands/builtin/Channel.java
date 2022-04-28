package ru.yandex.cloud.ml.platform.lzy.commands.builtin;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

import java.net.URI;
import java.util.Base64;
import java.util.UUID;

public final class Channel implements LzyCommand {
    private static final Logger LOG = LogManager.getLogger(Channel.class);
    private static final Options options = new Options();

    /*
     * ~$ channel <common-opts> channel-cmd channel-name -c content-type -t channel-type -s snapshot-id -e entry-id
     *
     *   argNo:                  #1          #2           <parsed>
     */

    static {
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
              + "channel <common-opts> cmd [name] [-c content-type] [-t channel-type] [-s snapshot-id] [-e entry-id]");
        }

        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("channel", options);
            return -1;
        }

        final String channelCommand = command.getArgList().get(1);
        String channelName = command.getArgList().size() > 2 ? command.getArgList().get(2) : null;

        final URI serverAddress = URI.create("grpc://" + command.getOptionValue('z'));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddress.getHost(), serverAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();

        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);

        switch (channelCommand) {
            case "create": {
                if (channelName == null) {
                    channelName = UUID.randomUUID().toString();
                }

                final Channels.ChannelCreate.Builder createCommandBuilder = Channels.ChannelCreate.newBuilder();

                if (localCmd.hasOption('c')) {
                    createCommandBuilder.setContentType(command.getOptionValue('c'));
                }

                if ("snapshot".equals(localCmd.getOptionValue('t'))) {
                    createCommandBuilder.setSnapshot(
                        Channels.SnapshotChannelSpec.newBuilder()
                            .setSnapshotId(localCmd.getOptionValue('s'))
                            .setEntryId(localCmd.getOptionValue('e'))
                            .build());
                } else {
                    createCommandBuilder.setDirect(Channels.DirectChannelSpec.newBuilder().build());
                }

                final Channels.ChannelCommand channelReq = Channels.ChannelCommand.newBuilder()
                    .setAuth(auth)
                    .setChannelName(channelName)
                    .setCreate(createCommandBuilder)
                    .build();

                final Channels.ChannelStatus channel = server.channel(channelReq);
                System.out.println(channel.getChannel().getChannelId());

                break;
            }

            case "status": {
                if (channelName == null) {
                    throw new IllegalArgumentException("Specify a channel name");
                }

                final Channels.ChannelCommand channelReq = Channels.ChannelCommand.newBuilder()
                    .setAuth(auth)
                    .setChannelName(channelName)
                    .setState(Channels.ChannelState.newBuilder().build())
                    .build();

                try {
                    final Channels.ChannelStatus channelStatus = server.channel(channelReq);
                    System.out.println(JsonFormat.printer().print(channelStatus));
                } catch (StatusRuntimeException e) {
                    System.err.println(
                        "Got exception while channel status (status_code=" + e.getStatus().getCode() + ")");
                }

                break;
            }

            case "destroy": {
                if (channelName == null) {
                    throw new IllegalArgumentException("Specify a channel name");
                }

                final Channels.ChannelCommand channelReq = Channels.ChannelCommand.newBuilder()
                    .setAuth(auth)
                    .setChannelName(channelName)
                    .setDestroy(Channels.ChannelDestroy.newBuilder().build())
                    .build();

                try {
                    final Channels.ChannelStatus channelStatus = server.channel(channelReq);
                    System.out.println(JsonFormat.printer().print(channelStatus));
                    System.out.println("Channel destroyed");
                } catch (StatusRuntimeException e) {
                    System.err.println(
                        "Got exception while channel destroy (status_code=" + e.getStatus().getCode() + ")");
                }

                break;
            }

            default:
                throw new IllegalStateException("Unknown channel command: " + channelCommand);
        }

        return 0;
    }
}
