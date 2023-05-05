package ai.lzy.service.graph.execute;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.storage.StorageClientFactory;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWF;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

public class CheckCache implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String wfName;
    private final String execId;
    private final StorageClientFactory storages;
    private final Collection<LWF.Operation> operations;
    private final Supplier<StepResult> completeAction;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public CheckCache(ExecutionDao execDao, String wfName, String execId, StorageClientFactory storages,
                      Collection<LWF.Operation> operationsToExecute, Supplier<StepResult> completeAction,
                      Function<StatusRuntimeException, StepResult> failAction, Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.wfName = wfName;
        this.execId = execId;
        this.storages = storages;
        this.operations = operationsToExecute;
        this.completeAction = completeAction;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    private static String safePrintCollectionOfMessage(Collection<? extends MessageOrBuilder> messages) {
        return messages.stream().map(op -> safePrinter().printToString(op)).collect(Collectors.joining(", "));
    }

    @Override
    public StepResult get() {
        log.info("{} Looking for graph tasks results in cache: { wfName: {}, execId: {} }", logPrefix,
            wfName, execId);
        log.debug("{} Graph tasks descriptions: {}", logPrefix, safePrintCollectionOfMessage(operations));

        LMST.StorageConfig storageConfig;
        try {
            storageConfig = withRetries(log, () -> execDao.getStorageConfig(execId));
        } catch (Exception e) {
            log.error("{} Error while obtaining storage config from dao: {}", logPrefix, e.getMessage(), e);
            return StepResult.RESTART;
        }

        var storageClient = storages.provider(storageConfig).get();
        var cachedOps = new ArrayList<LWF.Operation>();

        var iterator = operations.iterator();
        while (iterator.hasNext()) {
            final var operation = iterator.next();
            try {
                var cached = !operation.getOutputSlotsList().isEmpty() && operation.getOutputSlotsList().stream()
                    .map(LWF.Operation.SlotDescription::getStorageUri).allMatch(uri -> {
                        try {
                            return storageClient.blobExists(URI.create(uri));
                        } catch (Exception e) {
                            log.error("{} Error while checking blob existence: { storageUri: {}, error: {} }",
                                logPrefix, uri, e.getMessage(), e);
                            throw new StatusRuntimeException(Status.INTERNAL.withDescription(
                                "Cannot check blob existence: " + e.getMessage()));
                        }
                    });

                if (cached) {
                    log.debug("{} Task '{}' already in cache... removed from graph", logPrefix,
                        safePrinter().printToString(operation));
                    iterator.remove();
                    cachedOps.add(operation);
                }
            } catch (StatusRuntimeException sre) {
                return failAction.apply(sre);
            }
        }

        log.debug("{} Cache was scanned: { foundTasksResults: {}, notFound: {} }", logPrefix,
            safePrintCollectionOfMessage(operations), safePrintCollectionOfMessage(cachedOps));

        if (operations.isEmpty()) {
            log.debug("{} All graph tasks results already presented in cache, nothing to execute...", logPrefix);
            return completeAction.get();
        }

        return StepResult.CONTINUE;
    }
}
