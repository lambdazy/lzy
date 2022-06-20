package ai.lzy.env.client;

import ai.lzy.common.GrpcConverter;
import ai.lzy.disk.DiskType;
import ai.lzy.disk.client.DiskGrpcClient;
import ai.lzy.priv.v1.LCES;
import ai.lzy.priv.v1.LDS;
import ai.lzy.priv.v1.LED;
import ai.lzy.priv.v1.LzyCachedEnvServiceGrpc;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CachedEnvGrpcClient implements CachedEnvClient {

    private static final Logger LOG = LogManager.getLogger(DiskGrpcClient.class);

    private final LzyCachedEnvServiceGrpc.LzyCachedEnvServiceBlockingStub cachedEnv;

    public CachedEnvGrpcClient(Channel channel) {
        this.cachedEnv = LzyCachedEnvServiceGrpc.newBlockingStub(channel);
    }

    public String saveEnvConfig(String workflowName, String dockerImage, String yamlConfig, DiskType diskType) {
        final LCES.SaveEnvConfigRequest request = LCES.SaveEnvConfigRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setDiskType(GrpcConverter.to(diskType))
            .setDockerImage(dockerImage)
            .setYamlConfig(yamlConfig)
            .build();
        LOG.debug("Send saveEnvConfig request {}", request);
        final LCES.SaveEnvConfigResponse response;
        try {
            response = cachedEnv.saveEnvConfig(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to save env config: {}", e.getStatus().toString(), e);
            throw new RuntimeException(e.getCause());
        }
        LOG.info("Got response from saveEnvConfig request");
        return response.getDisk().getDiskId();
    }

    @Override
    public void markEnvReady(String workflowName, String diskId) {
        final LCES.MarkEnvReadyRequest request = LCES.MarkEnvReadyRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setDiskId(diskId)
            .build();
        LOG.debug("Send markEnvReady request {}", request);
        final LCES.MarkEnvReadyResponse response;
        try {
            response = cachedEnv.markEnvReady(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to mark env ready: {}", e.getStatus().toString(), e);
            throw new RuntimeException(e.getCause());
        }
        LOG.info("Got response from markEnvReady request");
    }

}
