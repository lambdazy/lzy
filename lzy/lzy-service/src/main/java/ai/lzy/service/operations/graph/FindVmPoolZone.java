package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.util.Strings;

import java.util.Set;
import java.util.function.Supplier;

import static ai.lzy.allocator.vmpool.VmPoolClient.findZones;

public final class FindVmPoolZone extends ExecuteGraphContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final VmPoolServiceBlockingStub vmPoolClient;

    public FindVmPoolZone(ExecutionStepContext stepCtx, ExecuteGraphState state,
                          VmPoolServiceBlockingStub vmPoolClient)
    {
        super(stepCtx, state);
        this.vmPoolClient = vmPoolClient;
    }

    @Override
    public StepResult get() {
        if (vmPoolZone() != null) {
            log().debug("{} VM pool zone already found, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        log().info("{} Find VM pools zone according to graph tasks specs: { wfName: {}, execId: {} }", logPrefix(),
            wfName(), execId());

        var poolLabels = request().getOperationsList().stream().map(LWF.Operation::getPoolSpecName).toList();

        final Set<String> suitableZones;
        try {
            suitableZones = findZones(poolLabels, vmPoolClient);
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error while VmPoolClient::findZones call", sre);
        }

        if (suitableZones.isEmpty()) {
            log().error("{} Suitable zones set are empty", logPrefix());
        } else {
            log().debug("{} Found suitable zones: {}", logPrefix(), String.join(", ", suitableZones));
        }

        Supplier<StepResult> onError = () -> {
            log().error("{} Cannot find suitable zone for pools specified in graph tasks", logPrefix());
            return failAction().apply(Status.INVALID_ARGUMENT.withDescription("Cannot find zone").asRuntimeException());
        };

        final String foundZone;

        if (Strings.isBlank(request().getZone())) {
            var candidateZone = suitableZones.stream().findAny();
            if (candidateZone.isPresent()) {
                foundZone = candidateZone.get();
            } else {
                return onError.get();
            }
        } else {
            if (suitableZones.contains(request().getZone())) {
                foundZone = request().getZone();
            } else {
                return onError.get();
            }
        }

        log().debug("{} Save found VM pools zone in dao...", logPrefix());
        setVmPoolZone(foundZone);

        try {
            saveState();
        } catch (Exception e) {
            return retryableFail(e, "Cannot save data about found VM pools zone in dao", Status.INTERNAL
                .withDescription("Cannot find VM pools zone").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }
}
