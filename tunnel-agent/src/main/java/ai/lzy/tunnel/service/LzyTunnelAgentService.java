package ai.lzy.tunnel.service;

import ai.lzy.v1.tunnel.LzyTunnelAgentGrpc;
import ai.lzy.v1.tunnel.TA;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Singleton
public class LzyTunnelAgentService extends LzyTunnelAgentGrpc.LzyTunnelAgentImplBase {

    private static final Logger LOG = LogManager.getLogger(LzyTunnelAgentService.class);

    public static final String TUNNEL_SCRIPT_PATH = "/app/resources/scripts/tunnel.sh";

    @Override
    public void createTunnel(TA.CreateTunnelRequest request, StreamObserver<TA.CreateTunnelResponse> responseObserver) {
        LOG.info(
            "Create Tunnel, remote v6 {}, worker pod v4 {}, k8s pod cidr {}",
            request.getRemoteV6Address(),
            request.getWorkerPodV4Address(),
            request.getK8SV4PodCidr()
        );
        try {
            Process process = Runtime.getRuntime().exec(
                new String[]{
                    TUNNEL_SCRIPT_PATH,
                    request.getRemoteV6Address(),
                    request.getWorkerPodV4Address(),
                    request.getK8SV4PodCidr(),
                    "2>&1"
                }
            );
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream())
            );
            String s;
            while ((s = reader.readLine()) != null) {
                // TODO: think about better script output logging
                LOG.info(s);
            }
        } catch (IOException e) {
            LOG.error("Error reading output of script", e);
            responseObserver.onError(e);
        }
        responseObserver.onNext(TA.CreateTunnelResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
