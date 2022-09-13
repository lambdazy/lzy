package ai.lzy.servant.commands;

import ai.lzy.v1.common.LMS;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import org.apache.commons.cli.CommandLine;
import ai.lzy.fs.commands.LzyCommand;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.deprecated.LzyAuth;
import ai.lzy.v1.deprecated.LzyServantGrpc;
import ai.lzy.v1.deprecated.LzyZygote;
import ai.lzy.v1.deprecated.Servant;

public class TerminalStatus implements LzyCommand {

    @Override
    public int execute(CommandLine command) throws Exception {
        final ManagedChannel servantCh = ChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub terminal = LzyServantGrpc.newBlockingStub(servantCh);
        final Servant.ServantStatus status = terminal.status(LzyAuth.Empty.newBuilder().build());
        System.out.println(status.getStatus());
        for (LMS.SlotStatus slotStatus : status.getConnectionsList()) {
            System.out.println(JsonFormat.printer().print(slotStatus));
        }
        return 0;
    }
}
