package ai.lzy.fs;

import ai.lzy.model.UriScheme;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.v1.IAM;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class LzyFsApp {
    private static final Options options = new Options();

    static {
        // common lzy options
        options.addRequiredOption("z", "lzy-address", true, "Lzy server address [host:port]");
        options.addRequiredOption("ch", "channel-manager", true, "Lzy channel manager address [host:port]");
        options.addOption("w", "lzy-whiteboard", true, "Lzy Whiteboard address [host:port]");
        options.addOption("m", "lzy-mount", true, "Lzy FS mount point");

        // local grpc server
        options.addOption("h", "grpc-host", true, "Local gRPC-server host");
        options.addOption("p", "grpc-port", true, "Local gRPC-server port");

        // user auth
        options.addOption("u", "user", true, "User name");
        options.addOption("k", "private-key", true, "User private key file (either --private-key or --user-token)");
        options.addOption("j", "user-token", true, "User token (either --private-key or --user-token)");

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

        final String mountPoint = cmdLine.getOptionValue("lzy-mount", LzyFsServer.DEFAULT_MOUNT_POINT);
        final String grpcHost = cmdLine.getOptionValue("grpc-host", LzyFsServer.DEFAULT_HOST);
        final int grpcPort = Integer.parseInt(cmdLine.getOptionValue("grpc-port", "" + LzyFsServer.DEFAULT_PORT));

        final String whiteboardAddress = "grpc://" + cmdLine.getOptionValue("lzy-whiteboard", serverAddress);
        final String channelManagerAddress = "grpc://" + cmdLine.getOptionValue("channel-manager");

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
            final String userToken = cmdLine.getOptionValue("user-token");
            if (privateKeyPath == null && userToken == null) {
                System.err.println("Neither 'private-key' nor 'user-token' not set");
                System.exit(-1);
                return;
            }
            if (privateKeyPath != null && userToken != null) {
                System.err.println("Either 'private-key' or 'user-token' is allowed");
                System.exit(-1);
                return;
            }
            String token = userToken;
            if (privateKeyPath != null) {
                token = "";
                if (Files.exists(Path.of(privateKeyPath))) {
                    try (FileReader keyReader = new FileReader(privateKeyPath)) {
                        token = JwtUtils.legacyBuildJWT(userId, keyReader);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        System.exit(-1);
                        return;
                    }
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

        final LzyFsServer server = new LzyFsServer(
            servantId,
            mountPoint,
            new URI(UriScheme.LzyFs.scheme(), null, grpcHost, grpcPort, null, null, null),
            new URI("http", null, serverHost, serverPort, null, null, null),
            new URI(whiteboardAddress),
            new URI(channelManagerAddress),
            authBuilder.build());
        server.start();
        server.awaitTermination();
    }
}
