package ai.lzy.tunnel.service;

import ai.lzy.v1.tunnel.LzyTunnelAgentGrpc;
import ai.lzy.v1.tunnel.TA;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LzyTunnelAgentService extends LzyTunnelAgentGrpc.LzyTunnelAgentImplBase {

    public static final String TUNNEL_SCRIPT_PATH = "/app/resources/scripts/";

    @Override
    public void createTunnel(TA.CreateTunnelRequest request, StreamObserver<TA.CreateTunnelResponse> responseObserver) {
        System.out.println("Create Tunnel");
        System.out.println(request.toString());
        System.out.println();
        System.out.println();
        System.out.println();
        try {
            Process process = Runtime.getRuntime().exec(
                new String[]{
                    "/app/resources/scripts/tunnel.sh",
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
                System.out.println(s);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            responseObserver.onError(e);
        }
        responseObserver.onNext(TA.CreateTunnelResponse.getDefaultInstance());
        responseObserver.onCompleted();
        System.out.println("ended");
    }
}
