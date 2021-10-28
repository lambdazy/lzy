package ru.yandex.cloud.ml.platform.lzy.servant.commands;

import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.CommandLine;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public class TerminalStatus implements LzyCommand {
    @Override
    public int execute(CommandLine command) throws Exception {
        final ManagedChannel servantCh = ManagedChannelBuilder
            .forAddress("localhost", Integer.parseInt(command.getOptionValue('p')))
            .usePlaintext()
            .build();
        final LzyServantGrpc.LzyServantBlockingStub terminal = LzyServantGrpc.newBlockingStub(servantCh);
        final Servant.ServantStatus status = terminal.status(IAM.Empty.newBuilder().build());
        System.out.println(status.getStatus());
        for (Operations.SlotStatus slotStatus : status.getConnectionsList()) {
            System.out.println(JsonFormat.printer().print(slotStatus));
        }
        return 0;
    }
}
