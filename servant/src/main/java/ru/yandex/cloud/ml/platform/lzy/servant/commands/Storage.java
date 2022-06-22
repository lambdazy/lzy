package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;

public class Storage implements LzyCommand {

    private static final Options options = new Options();

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
            case "s3": {
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
            case "bucket": {
                Lzy.GetBucketRequest.Builder builder = Lzy.GetBucketRequest
                    .newBuilder()
                    .setAuth(auth);

                Lzy.GetBucketResponse resp = server.getBucket(builder.build());
                System.out.println(JsonFormat.printer().print(resp));
                return 0;
            }
            default: {
                throw new IllegalArgumentException("Wrong storage type: " + command.getArgs()[1]);
            }
        }
    }
}

