package ai.lzy.tunnel.service;

import ai.lzy.util.grpc.ProtoPrinter;
import ai.lzy.v1.tunnel.LzyTunnelAgentGrpc;
import ai.lzy.v1.tunnel.TA;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

@Singleton
public class LzyTunnelAgentService extends LzyTunnelAgentGrpc.LzyTunnelAgentImplBase {

    private static final Logger LOG = LogManager.getLogger(LzyTunnelAgentService.class);

    private final TunnelManager tunnelManager;

    @Inject
    public LzyTunnelAgentService(TunnelManager tunnelManager) {
        this.tunnelManager = tunnelManager;
    }

    @Override
    public void createTunnel(TA.CreateTunnelRequest request, StreamObserver<TA.CreateTunnelResponse> responseObserver) {
        LOG.info("Create Tunnel: {}", ProtoPrinter.safePrinter().shortDebugString(request));
        Exception validationResult = validate(request);
        if (validationResult != null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(validationResult.getMessage())
                .asException());
            return;
        }
        tunnelManager.createTunnel(request.getRemoteV6Address(), request.getWorkerPodV4Address(),
            request.getK8SV4PodCidr(), request.getTunnelIndex());
        responseObserver.onNext(TA.CreateTunnelResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTunnel(TA.DeleteTunnelRequest request, StreamObserver<TA.DeleteTunnelResponse> responseObserver) {
        tunnelManager.destroyTunnel();
        responseObserver.onNext(TA.DeleteTunnelResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Nullable
    private static Exception validate(TA.CreateTunnelRequest request) {
        var errors = new ArrayList<>();
        if (request.getTunnelIndex() > 255 || request.getTunnelIndex() < 0) {
            errors.add("Tunnel index must be within range [0, 255]");
        }
        if (!ValidationUtils.validateCIDR(request.getK8SV4PodCidr())) {
            errors.add("Incorrect IPv4 CIDR");
        }
        if (!ValidationUtils.validateIpV4(request.getWorkerPodV4Address())) {
            errors.add("Incorrect pod v4 address");
        }
        if (!ValidationUtils.validateIpV6(request.getRemoteV6Address())) {
            errors.add("Incorrect remote v6 address");
        }
        if (errors.isEmpty()) {
            return null;
        } else {
            String errorsString = errors.stream().map(x -> "'" + x + "'")
                .collect(Collectors.joining(","));
            return new IllegalArgumentException("Validation errors: " + errorsString);
        }
    }
}
