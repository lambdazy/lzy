package ru.yandex.cloud.ml.platform.lzy.graph.db;

import java.util.Set;
import java.util.concurrent.Future;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

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
    void updateAtomic(String workflowId, String graphExecutionId, Mapper mapper) throws GraphDaoException;

    /**
     * Filter GraphExecutions by statuses, select oldest and update it by mapper
     * @param statuses Statuses to filter GraphExecutions by
     * @param mapper Function to update execution
     * @throws GraphDaoException Error while process mapping
     */
    void updateAtomic(Set<GraphExecutionState.Status> statuses, Mapper mapper) throws GraphDaoException;

    interface Mapper {
        GraphExecutionState update(@Nullable GraphExecutionState state) throws Exception;
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
