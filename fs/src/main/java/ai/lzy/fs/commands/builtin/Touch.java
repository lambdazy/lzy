package ai.lzy.fs.commands.builtin;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v2.*;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

public final class Touch implements LzyCommand {

    private static final Options options = new Options();

    /*
     * ~$ touch <common-opts> slot-path-to-create channel-name-to-attach-to --slot slot-description
     *
     *   argNo:                #1                  #2                        #3     #4
     */

    static {
        options.addRequiredOption("s", "slot", true, "Slot definition");
    }

    @SuppressWarnings("CheckStyle")
    public int execute(CommandLine command) throws Exception {
        if (command.getArgList().size() < 5 || !command.getArgList().get(3).equals("--slot")) {
            throw new IllegalArgumentException(
                "Invalid call format. Expected `touch <common-opts> slot-path channel-name --slot slot-description`.");
        }

        final String slotPath = command.getArgList().get(1);
        final String channelName = command.getArgList().get(2);
        final String slotDescr = command.getArgList().get(4);

        final Path lzyFsRoot = Path.of(command.getOptionValue('m'));

        final ManagedChannel lzyFsChannel = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyFsGrpc.SERVICE_NAME)
            .build();

        final LzyFsGrpc.LzyFsBlockingStub lzyFs = LzyFsGrpc.newBlockingStub(lzyFsChannel);

        final Operations.Slot.Builder slotBuilder = Operations.Slot.newBuilder();
        slotBuilder.setName("/" + getSlotRelPath(slotPath, lzyFsRoot));

        if (slotDescr.endsWith(".json")) {
            // treat as file
            JsonFormat.parser().merge(Files.newBufferedReader(Paths.get(slotDescr)), slotBuilder);
        } else if (slotDescr.endsWith("}")) {
            // read as direct json definition
            JsonFormat.parser().merge(slotDescr, slotBuilder);
        } else {
            // direct command
            final URI lzyServerAddress = URI.create("grpc://" + command.getOptionValue('z'));
            final IAM.Auth lzyServerAuth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

            final ManagedChannel lzyServerChannel = ChannelBuilder
                .forAddress(lzyServerAddress.getHost(), lzyServerAddress.getPort())
                .usePlaintext()
                .enableRetry(LzyServerGrpc.SERVICE_NAME)
                .build();
            final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(lzyServerChannel);

            final Channels.ChannelCommand channelCommand = Channels.ChannelCommand.newBuilder()
                .setAuth(lzyServerAuth)
                .setChannelName(channelName)
                .setState(Channels.ChannelState.newBuilder().build())
                .build();
            final Channels.ChannelStatus channelStatus = server.channel(channelCommand);

            slotBuilder.setContentType(channelStatus.getChannel().getContentType());
            switch (slotDescr) {
                case "input", "inpipe" -> {
                    slotBuilder.setMedia(Operations.Slot.Media.PIPE);
                    slotBuilder.setDirection(Operations.Slot.Direction.INPUT);
                }
                case "infile" -> {
                    slotBuilder.setMedia(Operations.Slot.Media.FILE);
                    slotBuilder.setDirection(Operations.Slot.Direction.INPUT);
                }
                case "output", "outpipe" -> {
                    slotBuilder.setMedia(Operations.Slot.Media.PIPE);
                    slotBuilder.setDirection(Operations.Slot.Direction.OUTPUT);
                }
                case "outfile" -> {
                    slotBuilder.setMedia(Operations.Slot.Media.FILE);
                    slotBuilder.setDirection(Operations.Slot.Direction.OUTPUT);
                }
                default -> throw new IllegalStateException("Unexpected slot description value: " + slotDescr);
            }
        }

        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        var request = LzyFsApi.CreateSlotRequest.newBuilder()
            .setTaskId(auth.hasTask() ? auth.getTask().getTaskId() : "user-" + auth.getUser().getUserId())
            .setSlot(slotBuilder.build())
            .setChannelId(channelName)
            .build();

        final LzyFsApi.SlotCommandStatus status = lzyFs.createSlot(request);
        System.out.println(JsonFormat.printer().print(status));

        return 0;
    }

    private static Path getSlotRelPath(String slotPath, Path lzyFsRoot) {
        final Path slotAbsPath = Path.of(slotPath).toAbsolutePath();
        if (!slotAbsPath.startsWith(lzyFsRoot)) {
            throw new IllegalArgumentException("Slot path must be in lzy-fs: " + lzyFsRoot);
        }

        final Path slotRelPath = lzyFsRoot.relativize(slotAbsPath);
        if (LzyFSManager.roots().stream().anyMatch(p -> slotRelPath.startsWith("/" + p))) {
            throw new IllegalArgumentException("System paths: " + LzyFSManager.roots()
                + " are prohibited for slots declaration");
        }

        return slotRelPath;
    }
}
