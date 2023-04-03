package ai.lzy.graph.algo;

import ai.lzy.graph.model.ChannelDescription;
import ai.lzy.graph.model.GraphDescription;
import ai.lzy.graph.model.TaskDescription;

public interface GraphBuilder {
    Integer MAX_VERTEXES = 100;

    void validate(GraphDescription graph) throws GraphValidationException;
    DirectedGraph<TaskVertex, ChannelEdge> build(GraphDescription graph) throws GraphValidationException;

    class GraphValidationException extends Exception {
        public GraphValidationException(String message) {
            super(message);
        }
    }

    class TaskVertex extends DirectedGraph.Vertex {
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

    class ChannelEdge extends DirectedGraph.Edge<TaskVertex> {
        private final TaskVertex input;
        private final TaskVertex output;
        private final ChannelDescription channelDesc;

        public ChannelEdge(TaskVertex input, TaskVertex output,
                           ChannelDescription channelDesc)
        {
            this.input = input;
            this.output = output;
            this.channelDesc = channelDesc;
        }

        @Override
        public TaskVertex input() {
            return input;
        }

        @Override
        public TaskVertex output() {
            return output;
        }

        public ChannelDescription channelDesc() {
            return channelDesc;
        }
    }
}
