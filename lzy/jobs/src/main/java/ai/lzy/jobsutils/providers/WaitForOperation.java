package ai.lzy.jobsutils.providers;

import ai.lzy.jobsutils.JobService;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Map;


/**
 * Job provider to wait for remote operation.
 * See job parameters in {@link OperationDescription}
 */
@Singleton
public class WaitForOperation extends JobProviderBase<WaitForOperation.OperationDescription> {
    private static final Logger LOG = LogManager.getLogger(WaitForOperation.class);
    private final ApplicationContext context;
    private final JobService jobService;

    @Inject
    public WaitForOperation(ApplicationContext context, JobService jobService) {
        super(OperationDescription.class);
        this.context = context;
        this.jobService = jobService;
    }

    @Override
    protected void execute(OperationDescription desc) {
        final var channel = GrpcUtils.newGrpcChannel(desc.serviceAddress, LongRunningServiceGrpc.SERVICE_NAME);

        LongRunningServiceGrpc.LongRunningServiceBlockingStub stub = LongRunningServiceGrpc.newBlockingStub(channel);

        if (desc.token != null) {
            stub = GrpcUtils.newBlockingClient(
                stub, "operations-client", () -> desc.token
            );
        }

        if (desc.idempotencyKey != null) {
            stub = GrpcUtils.withIdempotencyKey(stub, desc.idempotencyKey);
        }

        final JobProvider provider;
        try {
            provider = (JobProvider) context.getBean(Class.forName(desc.nextJobProviderClassName));
        } catch (ClassNotFoundException e) {
            LOG.error("Cannot get provider for next op:", e);
            return;
        }

        try {
            var op = stub.get(LongRunning.GetOperationRequest.newBuilder()
                .setOperationId(desc.operationId)
                .build());

            if (!op.getDone()) {
                try {
                    jobService.create(this, desc, desc.period);
                } catch (SerializationException e) {
                    LOG.error("Cannot reinit operation {} job: ", desc.operationId, e);
                }
                return;
            }

            try {
                jobService.create(provider, new OperationResult(
                    desc.operationId,
                    op,
                    desc.meta,
                    Status.OK.getCode().value()
                ), null);
            } catch (SerializationException e) {
                LOG.error("Cannot init provider for next op: ", e);
            }
        } catch (StatusRuntimeException e) {
            LOG.error("Error while waiting for op {}", desc.operationId,  e);
            try {
                jobService.create(provider, new OperationResult(
                    desc.operationId,
                    null,
                    desc.meta,
                    e.getStatus().getCode().value()
                ), null);
            } catch (SerializationException ex) {
                LOG.error("Cannot init provider for next op: ", ex);
            }
        } catch (Exception e) {
            LOG.error("Error while waiting for op {}", desc.operationId,  e);
            try {
                jobService.create(provider, new OperationResult(
                    desc.operationId,
                    null,
                    desc.meta,
                    Status.INTERNAL.getCode().value()
                ), null);
            } catch (SerializationException ex) {
                LOG.error("Cannot init provider for next op: ", ex);
            }
        }
    }


    /**
     * Arguments for {@link WaitForOperation} provider
     * @param serviceAddress address of LongRunning service
     * @param operationId id of operation to await
     * @param period period of polling
     * @param nextJobProviderClassName job provider class name to consume result of operation.
     *                                Must consume {@link OperationResult} as input of job
     *                                 or be {@link OperationConsumer}.
     * @param idempotencyKey idempotency key of get call
     * @param token token to provide as iam token. Must live long enough
     * @param meta meta to provide to next job
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record OperationDescription(
        String serviceAddress,
        String operationId,
        Duration period,
        String nextJobProviderClassName,
        @Nullable String idempotencyKey,
        @Nullable String token,
        @Nullable Map<String, String> meta
        ) {}


    /**
     * Result of {@link WaitForOperation} provider
     * @param operationId id of operation
     * @param op resulting operation
     * @param meta meta from request
     * @param statusCode Integer {@link Status} code of execution.
     *                  Can be INTERNAL if an error happened while executing job
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record OperationResult(
        String operationId,
        @Nullable LongRunning.Operation op,
        @Nullable Map<String, String> meta,
        @Nullable Integer statusCode
        ) {}

    public static abstract class OperationConsumer extends JobProviderBase<OperationResult> {
        protected OperationConsumer() {
            super(OperationResult.class);
        }
    }
}
