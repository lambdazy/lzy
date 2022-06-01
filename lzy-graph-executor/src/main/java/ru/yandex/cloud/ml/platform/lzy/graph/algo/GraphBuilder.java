package ru.yandex.cloud.ml.platform.lzy.graph.algo;

import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphExecutionState;

import java.util.Set;

public interface GraphBuilder {
    Integer MAX_VERTEXES = 100;

    Set<String> getNextExecutionGroup(GraphExecutionState graphExecution) throws GraphValidationException;
    void validate(GraphDescription graph) throws GraphValidationException;

    class GraphValidationException extends Exception {
        public GraphValidationException(String message) {
            super(message);
        }
    }
}
