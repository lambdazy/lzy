package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import ru.yandex.cloud.ml.platform.lzy.servant.ServantCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Channels;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.net.URI;
import java.util.Base64;

public class TasksStatus implements ServantCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final IAM.Auth auth = IAM.Auth.parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final ManagedChannel serverCh = ManagedChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);
        final Tasks.TasksList tasksList = server.tasksStatus(auth);
        for (final Tasks.TaskStatus status : tasksList.getTasksList()) {
            System.out.println(JsonFormat.printer().print(status));
        }
        return 0;
    }
}
