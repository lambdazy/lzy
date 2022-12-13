package ai.lzy.channelmanager.grpc;

import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import jakarta.inject.Singleton;

import static ai.lzy.channelmanager.ChannelManagerApp.APP;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class SlotGrpcConnection {

    private final ManagedChannel channel;
    private final LzySlotsApiGrpc.LzySlotsApiBlockingStub slotApiBlockingStub;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub operationApiBlockingStub;

    SlotGrpcConnection(RenewableJwt credentials, HostAndPort address) {
        this.channel = newGrpcChannel(address, LzySlotsApiGrpc.SERVICE_NAME);
        this.slotApiBlockingStub = newBlockingClient(
            LzySlotsApiGrpc.newBlockingStub(channel), APP, () -> credentials.get().token());
        this.operationApiBlockingStub = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(channel), APP, () -> credentials.get().token());
    }

    public LzySlotsApiGrpc.LzySlotsApiBlockingStub slotApiBlockingStub() {
        return slotApiBlockingStub;
    }

    public LongRunningServiceGrpc.LongRunningServiceBlockingStub operationApiBlockingStub() {
        return operationApiBlockingStub;
    }

    void shutdown() {
        channel.shutdown();
    }

}
