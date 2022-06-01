package ru.yandex.cloud.ml.platform.lzy.graph_executor.db;

import java.util.Set;
import java.util.concurrent.Future;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;

import javax.annotation.Nullable;
import java.util.List;

public interface GraphExecutionDao {
    GraphExecutionState create(String workflowId, GraphDescription description) throws GraphDaoException;

    @Nullable
    GraphExecutionState get(String workflowId, String graphExecutionId) throws GraphDaoException;
    List<GraphExecutionState> filter(GraphExecutionState.Status status) throws GraphDaoException;
    List<GraphExecutionState> list(String workflowId) throws GraphDaoException;

    /**
     * Updates GraphExecution state in transaction to prevent other selections
     * @param workflowId workflow id
     * @param graphExecutionId graph id
     * @param mapper function to update execution
     */
    void updateAtomic(String workflowId, String graphExecutionId,
          Mapper mapper) throws GraphDaoException;

    /**
     * Filter GraphExecutions by statuses, maps all elements of result by mapper and save them in one transaction
     * @param statuses Statuses to filter GraphExecutions by
     * @param mapper Function to update execution
     * @param limit Limit of rows to execute
     * @throws GraphDaoException Error while process mapping
     */
    void updateListAtomic(Set<GraphExecutionState.Status> statuses, ParallelMapper mapper, int limit) throws GraphDaoException;

    interface Mapper {
        GraphExecutionState update(@Nullable GraphExecutionState state) throws Exception;
    }

    interface ParallelMapper {
        Future<GraphExecutionState> update(GraphExecutionState state);
    }

    class GraphDaoException extends Exception {
        public GraphDaoException(Exception e) {
            super(e);
        }

        public GraphDaoException(String e) {
            super(e);
        }
    }
}
