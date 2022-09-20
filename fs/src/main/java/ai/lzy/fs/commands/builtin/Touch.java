package ai.lzy.fs.commands.builtin;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.fs.fs.LzyFSManager;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.common.LMS;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsGrpc;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

public final class Touch implements LzyCommand {

    private static final Options options = new Options();

    /*
     * ~$ touch <common-opts> slot-path-to-create channel-id-to-attach-to --slot slot-description
     *
     *   argNo:                #1                  #2                      #3     #4
     */

    static {
        options.addRequiredOption("s", "slot", true, "Slot definition");
    }

    @SuppressWarnings("CheckStyle")
    public int execute(CommandLine command) throws Exception {
        if (command.getArgList().size() < 5 || !command.getArgList().get(3).equals("--slot")) {
            throw new IllegalArgumentException(
                "Invalid call format. Expected `touch <common-opts> slot-path channel-id --slot slot-description`.");
        }

        final String slotPath = command.getArgList().get(1);
        final String channelId = command.getArgList().get(2);
        final String slotDescr = command.getArgList().get(4);

        final Path lzyFsRoot = Path.of(command.getOptionValue('m'));

        final ManagedChannel lzyFsChannel = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyFsGrpc.SERVICE_NAME)
            .build();

        final LzyFsGrpc.LzyFsBlockingStub lzyFs = LzyFsGrpc.newBlockingStub(lzyFsChannel);

        final LMS.Slot.Builder slotBuilder = LMS.Slot.newBuilder();
        slotBuilder.setName("/" + getSlotRelPath(slotPath, lzyFsRoot));

        if (slotDescr.endsWith(".json")) {
            // treat as file
            JsonFormat.parser().merge(Files.newBufferedReader(Paths.get(slotDescr)), slotBuilder);
        } else if (slotDescr.endsWith("}")) {
            // read as direct json definition
            JsonFormat.parser().merge(slotDescr, slotBuilder);
        } else {
            // direct command
            switch (slotDescr) {
                case "input", "inpipe" -> {
                    slotBuilder.setMedia(LMS.Slot.Media.PIPE);
                    slotBuilder.setDirection(LMS.Slot.Direction.INPUT);
                }
                case "infile" -> {
                    slotBuilder.setMedia(LMS.Slot.Media.FILE);
                    slotBuilder.setDirection(LMS.Slot.Direction.INPUT);
                }
                case "output", "outpipe" -> {
                    slotBuilder.setMedia(LMS.Slot.Media.PIPE);
                    slotBuilder.setDirection(LMS.Slot.Direction.OUTPUT);
                }
                case "outfile" -> {
                    slotBuilder.setMedia(LMS.Slot.Media.FILE);
                    slotBuilder.setDirection(LMS.Slot.Direction.OUTPUT);
                }
                default -> throw new IllegalStateException("Unexpected slot description value: " + slotDescr);
            }
        }

        final LzyAuth.Auth auth = LzyAuth.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final String agentId = command.getOptionValue("agent-id");

        var request = LzyFsApi.CreateSlotRequest.newBuilder()
            .setTaskId(auth.hasTask() ? auth.getTask().getTaskId() : agentId)
            .setSlot(slotBuilder.build())
            .setChannelId(channelId)
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
