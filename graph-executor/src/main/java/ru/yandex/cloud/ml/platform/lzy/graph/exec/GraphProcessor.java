package ru.yandex.cloud.ml.platform.lzy.graph.exec;

import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;


/**
 * Class to encapsulate logic of executing graph by steps
 */
public interface GraphProcessor {

    /**
     * Makes next step of execution on graph
     * @param graph Graph to execute
     * @return new state of given graph
     */
    GraphExecutionState exec(GraphExecutionState graph);

    /**
     * Stops graph execution
     * @param graph Graph to stop
     * @return new state of given graph
     */
    GraphExecutionState stop(GraphExecutionState graph, String errorDescription);
}
