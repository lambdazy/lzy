package ru.yandex.cloud.ml.platform.lzy.graph.algo;

import static ru.yandex.cloud.ml.platform.lzy.graph.algo.Graph.*;

import java.util.*;

public class Graph<T extends Vertex> {
    private final Map<String, Set<T>> graph = new HashMap<>();
    private final Map<String, Set<T>> reversedGraph = new HashMap<>();
    private final Set<T> vertexes = new HashSet<>();

    public Set<T> children(String parent) {
        return graph.getOrDefault(parent, new HashSet<>());
    }

    public Set<T> parents(String child) {
        return reversedGraph.getOrDefault(child, new HashSet<>());
    }

    public void addEdges(Collection<Edge<T>> edges) {
        edges.forEach(this::addEdge);
    }

    public void addEdge(Edge<T> edge) {
        vertexes.add(edge.input());
        vertexes.add(edge.output());
        graph
            .computeIfAbsent(edge.input().name(), k -> new HashSet<>())
            .add(edge.output());
        reversedGraph
            .computeIfAbsent(edge.output().name(), k -> new HashSet<>())
            .add(edge.input());
    }

    public Set<T> vertexes() {
        return vertexes;
    }

    public record Edge<T extends Vertex>(T input, T output) {}

    public static abstract class Vertex {
        abstract String name();

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Vertex)){
                return false;
            }
            return Objects.equals(name(), ((Vertex) obj).name());
        }

        @Override
        public int hashCode() {
            return name().hashCode();
        }
    }
}
