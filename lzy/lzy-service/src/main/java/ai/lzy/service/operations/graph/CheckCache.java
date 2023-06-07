package ai.lzy.service.operations.graph;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.dao.ExecuteGraphState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.storage.StorageClient;
import ai.lzy.v1.workflow.LWF;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class CheckCache extends ExecuteGraphContextAwareStep implements Supplier<StepResult>, RetryableFailStep {
    private final StorageClient storageClient;
    private final Supplier<StepResult> completeAction;

    public CheckCache(ExecutionStepContext stepCtx, ExecuteGraphState state,
                      StorageClient storageClient, Supplier<StepResult> completeAction)
    {
        super(stepCtx, state);
        this.storageClient = storageClient;
        this.completeAction = completeAction;
    }

    @Override
    public StepResult get() {
        if (operationsToExecute() != null) {
            log().debug("{} Cache already scanned, skip step...", logPrefix());

            if (operationsToExecute().isEmpty()) {
                log().debug("{} All graph tasks results already presented in cache, nothing to execute...",
                    logPrefix());
                return completeAction.get();
            }

            return StepResult.ALREADY_DONE;
        }

        log().info("{} Looking for graph tasks results in cache: { wfName: {}, execId: {} }", logPrefix(), wfName(),
            execId());
        log().debug("{} Graph tasks descriptions: {}", logPrefix(), printer().shortDebugString(
            request().getOperationsList()));

        var operationsToExecute = new ArrayList<LWF.Operation>();
        var cachedOps = new ArrayList<LWF.Operation>();

        for (LWF.Operation operation : request().getOperationsList()) {
            try {
                var cached = !operation.getOutputSlotsList().isEmpty() && operation.getOutputSlotsList().stream()
                    .map(LWF.Operation.SlotDescription::getStorageUri).allMatch(uri -> {
                        try {
                            return storageClient.blobExists(URI.create(uri));
                        } catch (Exception e) {
                            log().error("{} Error while checking blob existence: { storageUri: {}, error: {} }",
                                logPrefix(), uri, e.getMessage(), e);
                            throw new StatusRuntimeException(Status.INTERNAL.withDescription(
                                "Cannot check blob existence: " + e.getMessage()));
                        }
                    });

                if (cached) {
                    log().debug("{} Task '{}' already in cache... removed from graph", logPrefix(),
                        debugStringForOperation(operation));
                    cachedOps.add(operation);
                } else {
                    log().debug("{} Task '{}' not found in cache...", logPrefix(), debugStringForOperation(operation));
                    operationsToExecute.add(operation);
                }
            } catch (StatusRuntimeException sre) {
                return retryableFail(sre, "Error while processing cache to find cached tasks results", sre);
            }
        }

        log().debug("{} Cache was scanned: { foundTasksResults: {}, notFound: {} }", logPrefix(),
            debugStringForOperations(cachedOps), debugStringForOperations(operationsToExecute));

        log().debug("{} Save data about cached tasks results in dao...", logPrefix());
        setOperationsToExecute(operationsToExecute);

        try {
            saveState();
        } catch (Exception e) {
            return retryableFail(e, "Cannot save data about cached tasks results in dao", Status.INTERNAL
                .withDescription("Cannot process cache to find cached tasks results").asRuntimeException());
        }

        if (operationsToExecute.isEmpty()) {
            log().debug("{} All graph tasks results already presented in cache, nothing to execute...", logPrefix());
            return completeAction.get();
        }

        return StepResult.CONTINUE;
    }

    private String debugStringForOperations(Collection<LWF.Operation> operations) {
        return operations.stream().map(this::debugStringForOperation).collect(Collectors.joining(", ", "[", "]"));
    }

    private String debugStringForOperation(LWF.Operation operation) {
        return "{ name: " + operation.getName() + ", outputs: [" +
            printer().shortDebugString(operation.getOutputSlotsList()) + "] }";
    }
}
