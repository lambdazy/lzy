package ai.lzy.env;

import ai.lzy.env.service.CachedEnvService;
import ai.lzy.priv.v1.LCES;
import ai.lzy.priv.v1.LED;
import ai.lzy.priv.v2.LzyWorkflowApi;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CachedEnvServiceTest {

    private ApplicationContext ctx;
    private CachedEnvService envService;
    private Context grpcCtx;
    private Context prevGrpcCtx;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        envService = ctx.getBean(CachedEnvService.class);
        grpcCtx = Context.current();
        prevGrpcCtx = grpcCtx.attach();
    }

    @After
    public void tearDown() {
        grpcCtx.detach(prevGrpcCtx);
        ctx.stop();
    }

    @Test
    public void saveEnvConfigTest() {

        /*envService.saveEnvConfig(
            LCES.SaveEnvConfigRequest.newBuilder()
                .setWorkflowName("workflow-name-1")
                .setDockerImage("image")
                .setYamlConfig("yaml")
                .setDiskType(LED.DiskType.S3)
                .build(),
            new StreamObserver<>() {
                @Override
                public void onNext(LzyWorkflowApi.FinishWorkflowResponse value) {
                    assertEquals(LzyWorkflowApi.FinishWorkflowResponse.Status.NOT_FOUND.getNumber(), value.getStatus());
                }

                @Override
                public void onError(Throwable t) {
                    Assert.fail(t.getMessage());
                }

                @Override
                public void onCompleted() {
                }
            }
        );*/
    }

}
