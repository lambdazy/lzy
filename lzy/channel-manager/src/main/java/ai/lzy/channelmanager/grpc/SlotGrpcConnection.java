package ai.lzy.channelmanager.grpc;

import ai.lzy.channelmanager.ChannelManagerMain;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.slots.LzySlotsApiGrpc;
import ai.lzy.v1.slots.LzySlotsApiGrpc.LzySlotsApiBlockingStub;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class SlotGrpcConnection {
    private final ManagedChannel channel;
    private final LzySlotsApiBlockingStub slotsApi;

    SlotGrpcConnection(RenewableJwt credentials, HostAndPort address) {
        this.channel = newGrpcChannel(address, LzySlotsApiGrpc.SERVICE_NAME);
        this.slotsApi = newBlockingClient(
            ai.lzy.v1.slots.LzySlotsApiGrpc.newBlockingStub(channel),
            ChannelManagerMain.APP, () -> credentials.get().token());
    }

    public LzySlotsApiBlockingStub SlotsApi() {
        return slotsApi;
    }

    void shutdown() {
        channel.shutdown();
    }
}
