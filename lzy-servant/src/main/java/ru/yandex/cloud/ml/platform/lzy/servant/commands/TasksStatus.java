package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.net.URI;
import java.util.Base64;

public class TasksStatus implements LzyCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final ManagedChannel serverCh = ChannelBuilder
                .forAddress(serverAddr.getHost(), serverAddr.getPort())
                .usePlaintext()
                .enableRetry(LzyKharonGrpc.SERVICE_NAME)
                .build();
        final LzyKharonGrpc.LzyKharonBlockingStub server = LzyKharonGrpc.newBlockingStub(serverCh);
        final Tasks.TasksList tasksList = server.tasksStatus(auth);
        for (final Tasks.TaskStatus status : tasksList.getTasksList()) {
            System.out.println(JsonFormat.printer().print(status));
        }
        return 0;
    }
}
