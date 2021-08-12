package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantCommand;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class Publish implements ServantCommand {
    public int execute(CommandLine command) throws Exception {
        if (command.getArgs().length < 2)
            throw new IllegalArgumentException("Please specify the name of the zygote to publish");
        if (command.getArgs().length < 3)
            throw new IllegalArgumentException("Please specify zygote declaration file in JSON format");
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final ManagedChannel channel = ManagedChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(channel);
        final Operations.Zygote.Builder zbuilder = Operations.Zygote.newBuilder();
        JsonFormat.parser().merge(Files.newBufferedReader(Paths.get(command.getArgs()[2])), zbuilder);
        final Operations.RegisteredZygote registered = server.publish(Lzy.PublishRequest.newBuilder()
            .setAuth(IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a'))).getUser())
            .setName(command.getArgs()[1])
            .setOperation(zbuilder.build())
            .build()
        );
        new Update().execute(command);
        System.out.println("Registered " + registered.getName());
        return 0;
    }
}
