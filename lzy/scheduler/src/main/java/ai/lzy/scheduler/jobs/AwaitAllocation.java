package ai.lzy.scheduler.jobs;

import ai.lzy.scheduler.JobService;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.JobsOperationDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.providers.WorkflowJobProvider;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse.VmEndpoint.VmEndpointType;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Singleton
public class AwaitAllocation extends WorkflowJobProvider<TaskState> {
    private final ManagedChannel allocatorChannel;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub opStub;


    protected AwaitAllocation(JobService jobService, TaskStateSerializer serializer,
                              JobsOperationDao opDao, ApplicationContext context, ServiceConfig config)
    {
        super(jobService, serializer, opDao, Allocate.class, AfterAllocation.class, context);
        allocatorChannel = GrpcUtils.newGrpcChannel(config.getAllocatorAddress(), LongRunningServiceGrpc.SERVICE_NAME);
        var token = config.getIam().createRenewableToken();
        opStub = GrpcUtils.newBlockingClient(LongRunningServiceGrpc.newBlockingStub(allocatorChannel),
            "allocatorOpStub", () -> token.get().token());
    }

    @Override
    protected TaskState exec(TaskState state, String operationId) throws JobProviderException {
        var opId = state.allocatorOperationId();

        try {
            var res = opStub.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(opId)
                .build());

            if (!res.getDone()) {
                reschedule(Duration.ofSeconds(1));
                return null;
            }

            if (res.hasError()) {
                var err = res.getError();
                logger.error("Error while allocating vm for task {}: {}", state.id(), err);

                fail(Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage("Cannot allocate vm")
                    .build());
                return null;
            }

            var vmDesc = res.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);

            logger.info("Vm allocated. Description is {}", JsonUtils.printRequest(vmDesc));

            var address = vmDesc.getEndpointsList()
                .stream()
                .filter(e -> e.getType().equals(VmEndpointType.INTERNAL_IP))
                .findFirst()
                .orElse(null);

            if (address == null) {
                logger.error("Not found internal address of allocated vm {} for op {}", vmDesc.getVmId(), operationId);

                fail(Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage("Cannot allocate vm")
                    .build());
                return null;
            }

            var pk = vmDesc.getMetadataMap().get("PUBLIC_KEY");

            if (pk == null) {
                logger.error("Not found public key im metadata for vm {} for op {}", vmDesc.getVmId(), operationId);

                fail(Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage("Cannot allocate vm")
                    .build());
                return null;
            }


            return state.copy()
                .workerHost(address.getValue())
                .workerPublicKey(pk)
                .build();
        } catch (InvalidProtocolBufferException e) {
            logger.error("Cannot unpack response of allocate op for operation {}", operationId, e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.getNumber())
                .setMessage("Cannot allocate vm")
                .build());
            return null;
        }
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {

        if (state.allocatorOperationId() != null) {
            try {
                opStub.cancel(LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(state.allocatorOperationId())
                    .build());
            } catch (Exception e) {
                logger.error("Cannot cancel allocate op {}", state.allocatorOperationId(), e);
            }
        }

        return state.copy()
            .allocatorOperationId(null)
            .build();
    }

    @PreDestroy
    public void close() throws InterruptedException {
        allocatorChannel.shutdown();
        allocatorChannel.awaitTermination(1, TimeUnit.SECONDS);
        allocatorChannel.shutdownNow();
        allocatorChannel.awaitTermination(1, TimeUnit.SECONDS);
    }

}
