package ai.lzy.servant.commands;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.v1.*;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;

import java.net.URI;
import java.util.Base64;
import java.util.Set;

public class Portal implements LzyCommand {

    /*
     * ~$ portal <common-opts> [start|stop|status]
     */

    @Override
    public int execute(CommandLine command) throws Exception {
        if (command.getArgList().size() != 1 || !Set.of("start", "stop", "status").contains(command.getArgs()[0])) {
            throw new IllegalArgumentException(
                "Invalid call format. Expected `portal <common-opts> [start|stop|status].");
        }

        var portalChannel = ChannelBuilder.forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyPortalGrpc.SERVICE_NAME)
            .build();
        var portal = LzyPortalGrpc.newBlockingStub(portalChannel);

        var channelManagerAddress = URI.create(command.getOptionValue("channel-manager"));
        var auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        var channelManagerChannel = ChannelBuilder
            .forAddress(channelManagerAddress.getHost(), channelManagerAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        var channelManager = LzyChannelManagerGrpc.newBlockingStub(channelManagerChannel);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            portalChannel.shutdown();
            channelManagerChannel.shutdown();
        }));

        return switch (command.getArgs()[0]) {
            case "start" -> startPortal(portal, channelManager, auth);
            case "stop" -> stopPortal(portal);
            default -> statusPortal(portal);
        };
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private int startPortal(
        LzyPortalGrpc.LzyPortalBlockingStub portal,
        LzyChannelManagerGrpc.LzyChannelManagerBlockingStub channelManager,
        IAM.Auth auth
    ) {
        System.out.println("Starting portal...");

        final String stdoutChannelId;
        try {
            stdoutChannelId = channelManager.create(ChannelManager.ChannelCreateRequest.newBuilder()
                .setWorkflowId("unknown workflow id")
                .setChannelSpec(Channels.ChannelSpec.newBuilder()
                    .setChannelName("portal:stdout")
                    .setContentType(Operations.DataScheme.newBuilder()
                        .setSchemeType(Operations.SchemeType.PLAIN)
                        .build())
                    .setDirect(Channels.DirectChannelType.getDefaultInstance())
                    .build())
                .build()).getChannelId();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("Failed to create stdout channel: " + e.getStatus());
        }

        final String stderrChannelId;
        try {
            stderrChannelId = channelManager.create(ChannelManager.ChannelCreateRequest.newBuilder()
                .setWorkflowId("unknown workflow id")
                .setChannelSpec(Channels.ChannelSpec.newBuilder()
                    .setChannelName("portal:stderr")
                    .setContentType(Operations.DataScheme.newBuilder()
                        .setSchemeType(Operations.SchemeType.PLAIN)
                        .build())
                    .setDirect(Channels.DirectChannelType.getDefaultInstance())
                    .build())
                .build()).getChannelId();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("Failed to create stderr channel: " + e.getStatus());
        }

        try {
            portal.start(LzyPortalApi.StartPortalRequest.newBuilder()
                .setStdoutChannelId(stdoutChannelId)
                .setStderrChannelId(stderrChannelId)
                .build());
            System.out.println("Portal started.");
            return 0;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
                System.err.println("Portal is already active.");
                return -2;
            } else {
                System.err.println("Request failed with code " + e.getStatus().getCode());
                e.getStatus().asException().printStackTrace(System.err);
                return -1;
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private int stopPortal(LzyPortalGrpc.LzyPortalBlockingStub portal) {
        System.out.println("Terminating portal...");
        try {
            portal.stop(Empty.getDefaultInstance());
            System.out.println("Portal terminated.");
            return 0;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                System.err.println("Portal is not active.");
                return -2;
            } else {
                System.err.println("Request failed with code " + e.getStatus().getCode());
                e.getStatus().asException().printStackTrace(System.err);
                return -1;
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private int statusPortal(LzyPortalGrpc.LzyPortalBlockingStub portal) {
        try {
            portal.status(Empty.getDefaultInstance());
            System.out.println("Portal is active.");
            return 0;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                System.out.println("Portal is not active.");
                return 0;
            } else {
                System.err.println("Request failed with code " + e.getStatus().getCode());
                e.getStatus().asException().printStackTrace(System.err);
                return -1;
            }
        }
    }
}
