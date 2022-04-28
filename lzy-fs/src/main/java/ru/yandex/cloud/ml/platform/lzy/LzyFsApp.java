package ru.yandex.cloud.ml.platform.lzy;

import org.apache.commons.cli.*;
import ru.yandex.cloud.ml.platform.lzy.model.utils.JwtCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class LzyFsApp {
    private static final String DEFAULT_LZY_MOUNT = "/tmp/lzy";
    private static final String DEFAULT_GRPC_PORT = "2135";
    private static final Options options = new Options();

    static {
        // common lzy options
        options.addRequiredOption("z", "lzy-address", true, "Lzy server address [host:port]");
        options.addOption("w", "lzy-whiteboard", true, "Lzy Whiteboard address [host:port]");
        options.addOption("m", "lzy-mount", true, "Lzy FS mount point");

        // local grpc server
        options.addOption("p", "grpc-port", true, "Local gRPC-server port");

        // user auth
        options.addOption("u", "user", true, "User name");
        options.addOption("k", "private-key", true, "User private key file");

        // servant auth
        options.addOption("s", "servant-id", true, "Servant id");
        options.addOption("t", "servant-token", true, "Servant auth token");
    }

    public static void main(final String[] args) throws IOException, InterruptedException, URISyntaxException {
        final HelpFormatter cliHelp = new HelpFormatter();
        final CommandLine cmdLine;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            cliHelp.printHelp("lzyfs", options);
            System.exit(-1);
            return;
        }

        final String serverAddress = cmdLine.getOptionValue("lzy-address");
        final String[] addressParts = serverAddress.split(":", 2);
        if (addressParts.length != 2) {
            System.err.println("'lzy-address' should be in [host:port] format");
            cliHelp.printHelp("lzyfs", options);
            System.exit(-1);
            return;
        }
        final String serverHost = addressParts[0];
        final int serverPort = Integer.parseInt(addressParts[1]);

        final String mountPoint = cmdLine.getOptionValue("lzy-mount", DEFAULT_LZY_MOUNT);
        final int grpcPort = Integer.parseInt(cmdLine.getOptionValue("grpc-port", DEFAULT_GRPC_PORT));

        final String whiteboardAddress = "grpc://" + cmdLine.getOptionValue("lzy-whiteboard", serverAddress);

        final String userId = cmdLine.getOptionValue("user");
        final String servantId = cmdLine.getOptionValue("servant-id");

        final IAM.Auth.Builder authBuilder = IAM.Auth.newBuilder();
        if (userId != null) {
            if (servantId != null) {
                System.err.println("Either 'user' or 'servan-id' is allowed");
                System.exit(-1);
                return;
            }
            final String privateKeyPath = cmdLine.getOptionValue("private-key");
            if (privateKeyPath == null) {
                System.err.println("'private-key' not set");
                System.exit(-1);
                return;
            }
            String token = "";
            if (Files.exists(Path.of(privateKeyPath))) {
                try (FileReader keyReader = new FileReader(privateKeyPath)) {
                    token = JwtCredentials.buildJWT(userId, keyReader);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    System.exit(-1);
                    return;
                }
            }
            authBuilder.setUser(IAM.UserCredentials.newBuilder()
                .setUserId(userId)
                .setToken(token)
                .build());
        } else {
            authBuilder.setTask(IAM.TaskCredentials.newBuilder()
                .setServantId(servantId)
                .setServantToken(cmdLine.getOptionValue("servant-token", ""))
                .build());
        }

        final LzyFsServer server = new LzyFsServer(UUID.randomUUID().toString(), mountPoint, serverHost, serverPort,
            whiteboardAddress, authBuilder.build(), grpcPort);
        server.awaitTermination();
    }
}
