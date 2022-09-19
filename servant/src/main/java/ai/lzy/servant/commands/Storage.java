package ai.lzy.servant.commands;

import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.deprecated.Lzy;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyKharonGrpc;
import ai.lzy.v1.deprecated.LzyServerGrpc;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;

import java.net.URI;
import java.util.Base64;

public class Storage implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        LzyAuth.Auth auth = LzyAuth.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));

        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify operation type");
        }

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);

        switch (command.getArgs()[1]) {
            case "s3" -> {
                if (command.getArgs().length < 3) {
                    throw new IllegalArgumentException("Please specify bucket name");
                }
                Lzy.GetS3CredentialsRequest.Builder builder = Lzy.GetS3CredentialsRequest
                    .newBuilder()
                    .setAuth(auth)
                    .setBucket(command.getArgs()[2]);

                Lzy.GetS3CredentialsResponse resp = server.getS3Credentials(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            case "bucket" -> {
                Lzy.GetBucketRequest.Builder builder = Lzy.GetBucketRequest
                    .newBuilder()
                    .setAuth(auth);

                Lzy.GetBucketResponse resp = server.getBucket(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            default -> {
                throw new IllegalArgumentException("Wrong storage type: " + command.getArgs()[1]);
            }
        }
    }
}

