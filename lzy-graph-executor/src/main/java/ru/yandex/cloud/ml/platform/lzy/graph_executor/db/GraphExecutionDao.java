package ru.yandex.cloud.ml.platform.lzy.graph_executor.db;

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
          Updater mapper) throws GraphDaoException;

    interface Updater {
        GraphExecutionState update(GraphExecutionState state) throws Exception;
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
