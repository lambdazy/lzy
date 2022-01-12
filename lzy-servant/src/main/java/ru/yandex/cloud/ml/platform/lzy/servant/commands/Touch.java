package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFSManager;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFSManager;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public class Touch implements LzyCommand {

    private static final Options options = new Options();

    static {
        options.addOption(new Option("s", "slot", true, "Slot definition"));
    }

    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify the name of file to create");
        }
        if (command.getArgs().length < 3) {
            throw new IllegalArgumentException("Please specify the name of channel to connect to");
        }
        final String channelName = command.getArgs()[2];

        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("channel", options);
            return -1;
        }

        final ManagedChannel terminalCh = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub terminal = LzyServantGrpc
            .newBlockingStub(terminalCh);
        final Servant.CreateSlotCommand.Builder createCommandBuilder = Servant.CreateSlotCommand
            .newBuilder();
        if (localCmd.hasOption('s')) {
            final Operations.Slot.Builder slotBuilder = Operations.Slot.newBuilder();
            {
                final Path originalPath = Paths.get(command.getArgs()[1]).toAbsolutePath();
                final Path lzyFsRoot = Paths.get(command.getOptionValue('m'));
                if (!originalPath.startsWith(lzyFsRoot)) {
                    throw new IllegalArgumentException("Slot path must be in lzy-fs: " + lzyFsRoot);
                }
                final Path relativePath = lzyFsRoot.relativize(originalPath);
                if (LzyFSManager.roots().stream().anyMatch(p -> relativePath.startsWith("/" + p))) {
                    throw new IllegalArgumentException("System paths: " + LzyFSManager.roots()
                        + " are prohibited for slots declaration");
                }

                slotBuilder.setName("/" + relativePath);
            }
            final String slotDefinition = localCmd.getOptionValue('s');
            if (slotDefinition.endsWith(".json")) { // treat as file
                JsonFormat.parser()
                    .merge(Files.newBufferedReader(Paths.get(slotDefinition)), slotBuilder);
            } else if (slotDefinition.endsWith("}")) { // read as direct json definition
                JsonFormat.parser().merge(slotDefinition, slotBuilder);
            } else { // direct command
                final URI serverAddr = URI.create(command.getOptionValue('z'));
                final IAM.Auth auth = IAM.Auth
                    .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
                final ManagedChannel serverCh = ChannelBuilder
                    .forAddress(serverAddr.getHost(), serverAddr.getPort())
                    .usePlaintext()
                    .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                    .build();
                final LzyKharonGrpc.LzyKharonBlockingStub server = LzyKharonGrpc
                    .newBlockingStub(serverCh);

                final Channels.ChannelCommand channelReq = Channels.ChannelCommand.newBuilder()
                    .setAuth(auth)
                    .setChannelName(channelName)
                    .setState(Channels.ChannelState.newBuilder().build())
                    .build();
                final Channels.ChannelStatus channelStatus = server.channel(channelReq);
                slotBuilder.setContentType(channelStatus.getChannel().getContentType());
                switch (slotDefinition) {
                    case "input":
                    case "inpipe": {
                        slotBuilder.setMedia(Operations.Slot.Media.PIPE);
                        slotBuilder.setDirection(Operations.Slot.Direction.INPUT);
                        break;
                    }
                    case "infile": {
                        slotBuilder.setMedia(Operations.Slot.Media.FILE);
                        slotBuilder.setDirection(Operations.Slot.Direction.INPUT);
                        break;
                    }
                    case "output":
                    case "outpipe": {
                        slotBuilder.setMedia(Operations.Slot.Media.PIPE);
                        slotBuilder.setDirection(Operations.Slot.Direction.OUTPUT);
                        break;
                    }
                    case "outfile": {
                        slotBuilder.setMedia(Operations.Slot.Media.FILE);
                        slotBuilder.setDirection(Operations.Slot.Direction.OUTPUT);
                        break;
                    }
                }
            }
            createCommandBuilder.setSlot(slotBuilder.build());
        } else {
            throw new IllegalArgumentException("Please provide a slot definition with -s option");
        }
        createCommandBuilder.setChannelId(channelName);

        final Servant.SlotCommandStatus status = terminal.configureSlot(
            Servant.SlotCommand.newBuilder().setCreate(createCommandBuilder.build()).build()
        );
        System.out.println(JsonFormat.printer().print(status));
        return 0;
    }
}
