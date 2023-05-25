package ai.lzy.graph.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DirectedGraph {
    static class Edge {
        private final Task input;
        private final Task output;

        Edge(Task input, Task output) {
            this.input = input;
            this.output = output;
        }
    }
    private final Map<String, Set<Edge>> graph = new HashMap<>();  // Map from vertexId to set of edges
    private final Map<String, Set<Edge>> reversedGraph = new HashMap<>();  // Map from vertexId to set of reversed edges
    private final Set<Task> vertexes = new HashSet<>();
}
