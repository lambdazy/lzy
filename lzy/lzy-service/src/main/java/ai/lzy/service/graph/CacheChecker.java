package ai.lzy.service.graph;


import ai.lzy.storage.StorageClient;
import ai.lzy.v1.workflow.LWF.Operation;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import io.grpc.Status;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;

import static ai.lzy.util.grpc.ProtoPrinter.safePrinter;

class CacheChecker {
    public static void removeCachedOps(GraphExecutionState state, StorageClient storageClient, Logger log) {
        if (state.getOperations().isEmpty()) {
            state.fail(Status.INVALID_ARGUMENT, "Empty graph");
            return;
        }

        var filteredOperations = new ArrayList<Operation>();

        for (var operation : state.getOperations()) {
            var cached = !operation.getOutputSlotsList().isEmpty() && operation.getOutputSlotsList().stream()
                .map(SlotDescription::getStorageUri).allMatch(uri -> {
                    try {
                        return storageClient.blobExists(URI.create(uri));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
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
