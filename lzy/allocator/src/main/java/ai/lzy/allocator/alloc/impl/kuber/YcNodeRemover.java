package ai.lzy.allocator.alloc.impl.kuber;

import ai.lzy.allocator.alloc.AllocatorMetrics;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.InstanceServiceGrpc;
import yandex.cloud.api.compute.v1.InstanceServiceGrpc.InstanceServiceBlockingStub;
import yandex.cloud.api.compute.v1.InstanceServiceOuterClass.DeleteInstanceRequest;
import yandex.cloud.api.operation.OperationOuterClass;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.utils.OperationTimeoutException;
import yandex.cloud.sdk.utils.OperationUtils;

import java.time.Duration;

@Singleton
@Primary
@Requires(property = "allocator.kuber-allocator.enabled", value = "true")
@Requires(property = "allocator.yc-credentials.enabled", value = "true")
public class YcNodeRemover implements NodeRemover {
    private static final Logger LOG = LogManager.getLogger(YcNodeRemover.class);

    private static final Duration REMOVE_DURATION = Duration.ofMinutes(3);

    private final AllocatorMetrics metrics;
    private final InstanceServiceBlockingStub instanceService;
    private final OperationServiceBlockingStub operationService;

    @Inject
    public YcNodeRemover(AllocatorMetrics metrics, ServiceFactory serviceFactory) {
        this.metrics = metrics;
        this.instanceService = serviceFactory.create(InstanceServiceBlockingStub.class,
            InstanceServiceGrpc::newBlockingStub);
        this.operationService = serviceFactory.create(OperationServiceBlockingStub.class,
            OperationServiceGrpc::newBlockingStub);
    }

    @Override
    public void removeNode(String vmId, String nodeName, String nodeInstanceId) {
        LOG.info("Removing YC node {} (instance {}, vm {})...", nodeName, nodeInstanceId, vmId);

        OperationOuterClass.Operation op;
        try {
            op = instanceService.delete(
                DeleteInstanceRequest.newBuilder()
                    .setInstanceId(nodeInstanceId)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                LOG.warn("YC node {} (instance {}, vm {}) not exists", nodeName, nodeInstanceId, vmId);
                return;
            } else {
                metrics.removeNodeError.inc();
                LOG.error("Cannot remove YC node {} (instance {}, vm {}): {}",
                    nodeName, nodeInstanceId, vmId, e.getStatus());
                throw e;
            }
        }

        try {
            LOG.info("Wait YC node {} (instance {}, vm {}) removal op {} ...",
                nodeName, nodeInstanceId, vmId, op.getId());

            op = OperationUtils.wait(operationService, op, REMOVE_DURATION);
            if (op.hasResponse()) {
                LOG.info("YC node {} (instance {}, vm {}) successfully removed", nodeName, nodeInstanceId, vmId);
            } else {
                metrics.removeNodeError.inc();
                LOG.error("YC node {} (instance {}, vm {}) was not removed: {}",
                    nodeName, nodeInstanceId, vmId, op);
            }
        } catch (OperationTimeoutException e) {
            metrics.removeNodeError.inc();
            LOG.error("YC node {} (instance {}, vm {}) removal was not completed in {}",
                nodeName, nodeInstanceId, vmId, REMOVE_DURATION);
        } catch (InterruptedException e) {
            metrics.removeNodeError.inc();
            LOG.error("YC node {} (instance {}, vm {}) removal was interrupted", nodeName, nodeInstanceId, vmId);
        }
    }
}
