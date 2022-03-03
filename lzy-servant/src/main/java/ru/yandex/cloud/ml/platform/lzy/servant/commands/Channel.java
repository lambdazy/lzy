package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

public class Channel implements LzyCommand {

    private static final Options options = new Options();

    static {
        options.addOption(new Option("c", "content-type", true, "Content type"));
        options.addOption(new Option("t", "channel-type", true, "Channel type (direct or snapshot)"));
        options.addOption(new Option("s", "snapshot-id", true,
            "Snapshot id. Must be set if channel type is `snapshot`"));
        options.addOption(new Option("e", "entry-id", true,
            "Snapshot entry id. Must be set if channel type is `snapshot`"));
    }

    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify channel command");
        }
        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("channel", options);
            return -1;
        }

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);
        switch (command.getArgs()[1]) {
            case "create": {
                String channelName;
                if (command.getArgs().length < 3) {
                    channelName = UUID.randomUUID().toString();
                } else {
                    channelName = command.getArgs()[2];
                }
                final Channels.ChannelCreate.Builder createCommandBuilder = Channels.ChannelCreate
                    .newBuilder();
                if (localCmd.hasOption('c')) {
                    createCommandBuilder.setContentType(command.getOptionValue('c'));
                }
                if (Objects.equals(localCmd.getOptionValue('t'), "snapshot")) {
                    createCommandBuilder.setSnapshot(
                        Channels.SnapshotChannelSpec.newBuilder()
                            .setSnapshotId(localCmd.getOptionValue('s'))
                            .setEntryId(localCmd.getOptionValue('e'))
                            .build()
                    );
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
                String channelName;
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Specify a channel name");
                }
                channelName = command.getArgs()[2];
                final Channels.ChannelCommand channelReq = Channels.ChannelCommand.newBuilder()
                    .setAuth(auth)
                    .setChannelName(channelName)
                    .setState(Channels.ChannelState.newBuilder().build())
                    .build();
                try {
                    final Channels.ChannelStatus channelStatus = server.channel(channelReq);
                    System.out.println(JsonFormat.printer().print(channelStatus));
                } catch (StatusRuntimeException e) {
                    System.out.println(
                        "Got exception while channel status (status_code=" + e.getStatus().getCode() + ")");
                }
                break;
            }
            case "destroy": {
                String channelName;
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Specify a channel name");
                }
                channelName = command.getArgs()[2];
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
                    System.out.println(
                        "Got exception while channel destroy (status_code=" + e.getStatus().getCode() + ")");
                }
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + command.getArgs()[1]);
        }
        return 0;
    }
}
