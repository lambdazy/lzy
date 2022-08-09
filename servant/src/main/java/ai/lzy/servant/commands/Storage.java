package ai.lzy.servant.commands;

import ai.lzy.model.grpc.ChannelBuilder;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.v1.IAM;
import ai.lzy.v1.Lzy;
import ai.lzy.v1.LzyKharonGrpc;
import ai.lzy.v1.LzyServerGrpc;

public class Storage implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        IAM.Auth auth = IAM.Auth
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

