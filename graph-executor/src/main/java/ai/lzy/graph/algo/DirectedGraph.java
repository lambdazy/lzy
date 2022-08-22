package ai.lzy.graph.algo;

import static ai.lzy.graph.algo.DirectedGraph.*;

import java.util.*;

public class DirectedGraph<T extends Vertex, E extends Edge<T>> {
    private final Map<String, Set<E>> graph = new HashMap<>();  // Map from vertexId to set of edges
    private final Map<String, Set<E>> reversedGraph = new HashMap<>();  // Map from vertexId to set of reversed edges
    private final Set<T> vertexes = new HashSet<>();

    public Set<E> children(String parent) {
        return graph.getOrDefault(parent, new HashSet<>());
    }

    public Set<E> parents(String child) {
        return reversedGraph.getOrDefault(child, new HashSet<>());
    }

    public void addEdges(Collection<E> edges) {
        edges.forEach(this::addEdge);
    }

    public void addEdge(E edge) {
        vertexes.add(edge.input());
        vertexes.add(edge.output());
        graph
            .computeIfAbsent(edge.input().name(), k -> new HashSet<>())
            .add(edge);
        reversedGraph
            .computeIfAbsent(edge.output().name(), k -> new HashSet<>())
            .add(edge);
    }

    public void addVertex(T vertex) {
        vertexes.add(vertex);
    }

    public Set<T> vertexes() {
        return vertexes;
    }

    public abstract static class Edge<T extends Vertex> {
        abstract T input();
        abstract T output();

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Edge)) {
                return false;
            }
            return Objects.equals(input(), ((Edge<?>) obj).input())
                && Objects.equals(output(), ((Edge<?>) obj).output());
        }

        @Override
        public int hashCode() {
            return Objects.hash(input(), output());
        }
    }

    public abstract static class Vertex {
        abstract String name();

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Vertex)) {
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
