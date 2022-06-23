package ai.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.commands.LzyCommand;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

public class Publish implements LzyCommand {

    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2) {
            throw new IllegalArgumentException(
                "Please specify zygote declaration file in JSON format");
        }
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel channel = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(channel);
        final Operations.Zygote.Builder zbuilder = Operations.Zygote.newBuilder();
        JsonFormat.parser()
            .merge(Files.newBufferedReader(Paths.get(command.getArgs()[1])), zbuilder);
        //noinspection ResultOfMethodCallIgnored
        server.publish(Lzy.PublishRequest.newBuilder()
            .setAuth(IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')))
                .getUser())
            .setOperation(zbuilder.build())
            .build()
        );
        new Update().execute(command);
        System.out.println("Registered " + zbuilder.getName());
        return 0;
    }
}
