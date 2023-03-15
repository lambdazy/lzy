package ai.lzy.tunnel;

import ai.lzy.tunnel.service.LzyTunnelAgentService;
import ai.lzy.tunnel.service.TunnelManager;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.tunnel.LzyTunnelAgentGrpc;
import ai.lzy.v1.tunnel.TA;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class LzyTunnelAgentServiceTest {
    private TunnelManager mockTunnelManager;
    private LzyTunnelAgentGrpc.LzyTunnelAgentBlockingStub lzyTunnelAgentStub;
    private ManagedChannel channel;

    @Before
    public void setup() throws IOException {
        mockTunnelManager = Mockito.mock(TunnelManager.class);
        LzyTunnelAgentService tunnelAgentService = new LzyTunnelAgentService(mockTunnelManager);
        int port = ai.lzy.test.GrpcUtils.rollPort();
        TunnelAgentMain main = new TunnelAgentMain(tunnelAgentService, "localhost:" + port);
        main.start();
        channel = GrpcUtils.newGrpcChannel("localhost", port, LzyTunnelAgentGrpc.SERVICE_NAME);
        lzyTunnelAgentStub = LzyTunnelAgentGrpc.newBlockingStub(channel);
    }

    @After
    public void destroy() {
        channel.shutdown();
    }

    @Test
    public void tunnelAgentShouldValidateCreateTunnelArgs() {
        StatusRuntimeException error = Assert.assertThrows(StatusRuntimeException.class, () -> {
            lzyTunnelAgentStub.createTunnel(TA.CreateTunnelRequest.newBuilder()
                .setTunnelIndex(256)
                .setK8SV4PodCidr("256.128.64.32/33")
                .setWorkerPodV4Address("1.2.2.3.4")
                .setRemoteV6Address("foooooooooooo")
                .build());
        });
        Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
        Assert.assertNotNull(error.getStatus().getDescription());
        Assert.assertTrue(error.getStatus().getDescription().contains("Incorrect IPv4 CIDR"));
        Assert.assertTrue(error.getStatus().getDescription().contains("Incorrect pod v4 address"));
        Assert.assertTrue(error.getStatus().getDescription().contains("Incorrect remote v6 address"));
        Assert.assertTrue(error.getStatus().getDescription().contains("Tunnel index must be within range [0, 255]"));
        Mockito.verify(mockTunnelManager, Mockito.times(0))
            .createTunnel(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void tunnelAgentShouldCallCreateTunnelOnManager() {
        lzyTunnelAgentStub.createTunnel(TA.CreateTunnelRequest.newBuilder()
            .setTunnelIndex(0)
            .setK8SV4PodCidr("1.1.1.1/24")
            .setWorkerPodV4Address("1.22.3.4")
            .setRemoteV6Address("fe80::")
            .build());
        Mockito.verify(mockTunnelManager, Mockito.times(1))
            .createTunnel(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void tunnelAgentShouldThrowExceptionOnCreateTunnelFail() {
        Mockito.doThrow(new RuntimeException("test error")).when(mockTunnelManager)
            .createTunnel(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt());
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class, () ->
            lzyTunnelAgentStub.createTunnel(TA.CreateTunnelRequest.newBuilder()
                .setTunnelIndex(0)
                .setK8SV4PodCidr("1.1.1.1/24")
                .setWorkerPodV4Address("1.22.3.4")
                .setRemoteV6Address("fe80::")
                .build()));
        Assert.assertEquals(Status.UNKNOWN.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void tunnelAgentShouldCallDeleteTunnelOnManager() {
        lzyTunnelAgentStub.deleteTunnel(TA.DeleteTunnelRequest.getDefaultInstance());
        Mockito.verify(mockTunnelManager, Mockito.times(1))
            .destroyTunnel();
    }

    @Test
    public void tunnelAgentShouldThrowExceptionOnDeleteTunnelFail() {
        Mockito.doThrow(new RuntimeException("test error")).when(mockTunnelManager).destroyTunnel();
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class, () ->
            lzyTunnelAgentStub.deleteTunnel(TA.DeleteTunnelRequest.getDefaultInstance()));
        Assert.assertEquals(Status.UNKNOWN.getCode(), exception.getStatus().getCode());
    }
}
