package ru.yandex.cloud.ml.platform.lzy.graph_executor.algo;

import java.util.*;

public class Graph {
    private final Map<String, Set<String>> graph = new HashMap<>();
    private final Map<String, Set<String>> reversedGraph = new HashMap<>();
    private final Set<String> vertexes = new HashSet<>();

    public Set<String> children(String parent) {
        return graph.getOrDefault(parent, new HashSet<>());
    }

    public Set<String> parents(String child) {
        return reversedGraph.getOrDefault(child, new HashSet<>());
    }

    public void addEdges(Collection<Edge> edges) {
        edges.forEach(this::addEdge);
    }

    public void addEdge(Edge edge) {
        vertexes.add(edge.input());
        vertexes.add(edge.output());
        graph.computeIfAbsent(edge.input(), k -> new HashSet<>()).add(edge.output());
        reversedGraph.computeIfAbsent(edge.output(), k -> new HashSet<>()).add(edge.input());
    }

    public Set<String> vertexes() {
        return vertexes;
    }

    public record Edge(String input, String output) {}
}
