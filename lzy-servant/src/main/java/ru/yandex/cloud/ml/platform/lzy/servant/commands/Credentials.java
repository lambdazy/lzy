package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.*;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;

import java.net.URI;
import java.util.Base64;

public class Credentials implements LzyCommand {

    private static final Options options = new Options();

    static {
        options.addOption(new Option("b", "return bucket", false, "Return bucket name assigned to user with credentials"));
    }

    @Override
    public int execute(CommandLine command) throws Exception {
        final CommandLine localCmd;
        final HelpFormatter cliHelp = new HelpFormatter();
        try {
            localCmd = new DefaultParser().parse(options, command.getArgs(), false);
        } catch (ParseException e) {
            cliHelp.printHelp("whiteboard", options);
            return -1;
        }
        final IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        if (!auth.hasUser()) {
            throw new IllegalArgumentException("Please provide user credentials");
        }

        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException("Please specify credentials type");
        }

        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel serverCh = ChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        LzyKharonGrpc.LzyKharonBlockingStub kharon = LzyKharonGrpc.newBlockingStub(serverCh);

        if (!localCmd.hasOption('b')) {
            switch (command.getArgs()[1]) {
                case "s3": {
                    if (command.getArgs().length < 3) {
                        throw new IllegalArgumentException("Please specify bucket name");
                    }

                    Lzy.GetS3CredentialsRequest.Builder builder = Lzy.GetS3CredentialsRequest
                            .newBuilder()
                            .setAuth(auth)
                            .setBucket(command.getArgs()[2]);

                    Lzy.GetS3CredentialsResponse resp = kharon.getS3Credentials(builder.build());
                    System.out.println(JsonFormat.printer().print(resp));
                    return 0;
                }
                default: {
                    System.out.println("Wrong credentials type");
                    return -1;
                }
            }
        } else {
            switch (command.getArgs()[1]) {
                case "s3": {
                    Lzy.GetS3CredentialsRequest.Builder builder = Lzy.GetS3CredentialsRequest
                            .newBuilder()
                            .setAuth(auth);

                    Lzy.GetS3CredentialsAndBucketResponse resp = kharon.getS3CredentialsAndBucket(builder.build());
                    System.out.println(JsonFormat.printer().print(resp));
                    return 0;
                }
                default: {
                    System.out.println("Wrong credentials type");
                    return -1;
                }
            }
        }
    }
}
