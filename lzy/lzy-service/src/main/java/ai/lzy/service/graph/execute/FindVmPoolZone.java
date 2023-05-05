package ai.lzy.service.graph.execute;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.v1.VmPoolServiceGrpc.VmPoolServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.allocator.vmpool.VmPoolClient.findZones;

public class FindVmPoolZone implements Supplier<StepResult> {
    private final VmPoolServiceBlockingStub vmPoolClient;
    private final String wfName;
    private final String execId;
    private final String requiredZone;
    private final Collection<String> poolLabels;
    private final Consumer<String> zoneConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public FindVmPoolZone(String wfName, String execId, @Nullable String requiredZone, Collection<String> poolLabels,
                          VmPoolServiceBlockingStub vmPoolClient, Consumer<String> zoneConsumer,
                          Function<StatusRuntimeException, StepResult> failAction, Logger log, String logPrefix)
    {
        this.wfName = wfName;
        this.execId = execId;
        this.requiredZone = requiredZone;
        this.poolLabels = poolLabels;
        this.vmPoolClient = vmPoolClient;
        this.zoneConsumer = zoneConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Finding VM pools zone according to graph tasks specs: { wfName: {}, execId: {} }", logPrefix,
            wfName, execId);

        Set<String> suitableZones;
        try {
            suitableZones = findZones(poolLabels, vmPoolClient);
        } catch (StatusRuntimeException sre) {
            log.error("{} Error while VmPoolClient::findZones call: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        if (suitableZones.isEmpty()) {
            log.debug("{} Found suitable zones: {}", logPrefix, String.join(", ", suitableZones));
        } else {
            log.error("{} Suitable zones set are empty", logPrefix);
        }

        if (Strings.isBlank(requiredZone)) {
            var candidateZone = suitableZones.stream().findAny();
            if (candidateZone.isPresent()) {
                zoneConsumer.accept(candidateZone.get());
                return StepResult.CONTINUE;
            }
        } else if (suitableZones.contains(requiredZone)) {
            zoneConsumer.accept(requiredZone);
            return StepResult.CONTINUE;
        }

        log.error("{} Cannot find suitable zone for pools specified in graph tasks", logPrefix);
        return failAction.apply(Status.INVALID_ARGUMENT.withDescription("Cannot find zone").asRuntimeException());
    }
}
