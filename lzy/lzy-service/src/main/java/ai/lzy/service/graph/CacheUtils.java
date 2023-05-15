package ai.lzy.service.graph;


import ai.lzy.service.dao.GraphExecutionState;
import ai.lzy.storage.StorageClient;
import ai.lzy.v1.workflow.LWF.Operation;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;

import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

class CacheUtils {
    public static void removeCachedOps(GraphExecutionState state, StorageClient storageClient, Logger log) {
        if (state.getOperations().isEmpty()) {
            state.fail(Status.INVALID_ARGUMENT, "Empty graph");
            return;
        }

        var filteredOperations = new ArrayList<Operation>();

        for (var operation : state.getOperations()) {
            var cached = !operation.getOutputSlotsList().isEmpty() && operation.getOutputSlotsList().stream()
                .map(SlotDescription::getStorageUri)
                .filter(uri -> !uri.endsWith("exception"))
                .allMatch(uri -> {
                    try {
                        return storageClient.blobExists(URI.create(uri));
                    } catch (Exception e) {
                        log.error("Error while checking blob existence: { storageUri: {} }", uri);
                        throw new StatusRuntimeException(Status.INTERNAL.withDescription(
                            "Cannot check blob existence"));
                    }
                });

            if (cached) {
                log.debug("Op '{}' already in cache... remove from graph", safePrinter().printToString(operation));
                continue;
            }

            filteredOperations.add(operation);
        }

        state.setOperations(filteredOperations);
    }
}
