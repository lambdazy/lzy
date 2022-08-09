package ai.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.Base64;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.util.ChannelBuilder;
import ai.lzy.v1.IAM;
import ai.lzy.v1.LzyKharonGrpc;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.Tasks;

public class TasksStatus implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final URI serverAddr = URI.create(command.getOptionValue('z'));
        final IAM.Auth auth = IAM.Auth
            .parseFrom(Base64.getDecoder().decode(command.getOptionValue('a')));
        final ManagedChannel serverCh = ChannelBuilder
            .forAddress(serverAddr.getHost(), serverAddr.getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        final LzyServerGrpc.LzyServerBlockingStub server = LzyServerGrpc.newBlockingStub(serverCh);
        final Tasks.TasksList tasksList = server.tasksStatus(auth);
        System.out.print(JsonFormat.printer().print(tasksList));
        return 0;
    }
}
