package ai.lzy.servant.commands;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v2.*;
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

        var serverAddr = URI.create(command.getOptionValue('z'));
        var auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        var serverChannel = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        var server = LzyServerGrpc.newBlockingStub(serverChannel);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            portalChannel.shutdown();
            serverChannel.shutdown();
        }));

        return switch (command.getArgs()[0]) {
            case "start" -> startPortal(portal, server, auth);
            case "stop" -> stopPortal(portal);
            default -> statusPortal(portal);
        };
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private int startPortal(LzyPortalGrpc.LzyPortalBlockingStub portal, LzyServerGrpc.LzyServerBlockingStub server,
                            IAM.Auth auth) {
        System.out.println("Starting portal...");

        try {
            server.channel(Channels.ChannelCommand.newBuilder()
                .setAuth(auth)
                .setChannelName("portal:stdout")
                .setCreate(Channels.ChannelCreate.newBuilder()
                    .setContentType(Operations.DataScheme.newBuilder()
                        .setSchemeType(Operations.SchemeType.plain)
                        .build())
                    .setDirect(Channels.DirectChannelSpec.getDefaultInstance())
                    .build())
                .build());
        } catch (StatusRuntimeException e) {
            System.err.println("Failed to create stdout channel: " + e.getStatus());
            e.printStackTrace(System.err);
        }

        try {
            server.channel(Channels.ChannelCommand.newBuilder()
                .setAuth(auth)
                .setChannelName("portal:stderr")
                .setCreate(Channels.ChannelCreate.newBuilder()
                    .setContentType(Operations.DataScheme.newBuilder()
                        .setSchemeType(Operations.SchemeType.plain)
                        .build())
                    .setDirect(Channels.DirectChannelSpec.getDefaultInstance())
                    .build())
                .build());
        } catch (StatusRuntimeException e) {
            System.err.println("Failed to create stderr channel: " + e.getStatus());
            e.printStackTrace(System.err);
        }

        try {
            portal.start(LzyPortalApi.StartPortalRequest.newBuilder()
                .setStdoutChannelId("portal:stdout")
                .setStderrChannelId("portal:stderr")
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
