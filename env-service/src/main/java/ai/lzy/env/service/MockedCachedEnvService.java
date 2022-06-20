package ai.lzy.env.service;

import ai.lzy.priv.v1.LCES;
import ai.lzy.priv.v1.LED;
import ai.lzy.priv.v1.LzyCachedEnvServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MockedCachedEnvService extends LzyCachedEnvServiceGrpc.LzyCachedEnvServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(MockedCachedEnvService.class);

    @Override
    public void saveEnvConfig(
        LCES.SaveEnvConfigRequest request,
        StreamObserver<LCES.SaveEnvConfigResponse> responseObserver
    ) {
        LOG.debug("Mocking saveEnvConfig request");
        responseObserver.onNext(LCES.SaveEnvConfigResponse.newBuilder()
            .setEnvId("env")
            .setDisk(LED.Disk.newBuilder().setDiskId("disk").setType(request.getDiskType()).build())
            .build()
        );
        responseObserver.onCompleted();
    }
}
