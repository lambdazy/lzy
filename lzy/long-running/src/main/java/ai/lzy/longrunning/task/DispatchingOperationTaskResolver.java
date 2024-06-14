package ai.lzy.longrunning.task;

import ai.lzy.model.db.TransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DispatchingOperationTaskResolver implements OperationTaskResolver {

    private static final Logger LOG = LogManager.getLogger(DispatchingOperationTaskResolver.class);
    private final Map<String, TypedOperationTaskResolver> resolvers;

    public DispatchingOperationTaskResolver(List<TypedOperationTaskResolver> resolvers) {
        this.resolvers = generateResolversMap(resolvers);
    }

    private static Map<String, TypedOperationTaskResolver> generateResolversMap(
        List<TypedOperationTaskResolver> resolvers)
    {
        var types = new HashSet<String>();
        resolvers.forEach(r -> {
            if (!types.add(r.type())) {
                throw new IllegalStateException("Duplicate resolver for type " + r.type());
            }
        });
        return resolvers.stream()
            .collect(Collectors.toMap(TypedOperationTaskResolver::type, r -> r));
    }

    @VisibleForTesting
    void addResolver(TypedOperationTaskResolver resolver) {
        resolvers.put(resolver.type(), resolver);
    }

    @Override
    public Result resolve(OperationTask operationTask, @Nullable TransactionHandle tx) throws SQLException {
        var resolver = resolvers.get(operationTask.type());
        if (resolver == null) {
            LOG.error("No resolver for task type {}. Task: {}", operationTask.type(), operationTask);
            return Result.UNKNOWN_TASK;
        }
        try {
            return resolver.resolve(operationTask, tx);
        } catch (Exception e) {
            LOG.error("Error while resolving task {}", operationTask.id(), e);
            return Result.resolveError(e);
        }
    }
}
