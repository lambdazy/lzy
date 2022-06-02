package ru.yandex.cloud.ml.platform.lzy.graph.algo;

import ru.yandex.cloud.ml.platform.lzy.graph.algo.Graph.Vertex;
import ru.yandex.cloud.ml.platform.lzy.graph.model.GraphDescription;
import ru.yandex.cloud.ml.platform.lzy.graph.model.TaskDescription;

public interface GraphBuilder {
    Integer MAX_VERTEXES = 100;

    void validate(GraphDescription graph) throws GraphValidationException;
    Graph<TaskVertex> build(GraphDescription graph) throws GraphValidationException;

    class GraphValidationException extends Exception {
        public GraphValidationException(String message) {
            super(message);
        }
    }

    class TaskVertex extends Vertex {
        private final String name;
        private final TaskDescription description;

        public TaskVertex(String name, TaskDescription description) {
            this.name = name;
            this.description = description;
        }

        @Override
        String name() {
            return name;
        }

        public TaskDescription description() {
            return description;
        }
    }
}
