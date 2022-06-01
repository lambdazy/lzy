package ru.yandex.cloud.ml.platform.lzy.graph_executor.exec;

import ru.yandex.cloud.ml.platform.lzy.graph_executor.model.GraphExecutionState;

public interface GraphProcessor {

    /**
     * Makes next step of execution on graph
     * @param graph Graph to execute
     * @return new state of given graph
     */
    GraphExecutionState exec(GraphExecutionState graph);
}
