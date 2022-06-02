package ru.yandex.cloud.ml.platform.lzy.graph.algo;

import java.util.*;
import java.util.function.Function;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.DirectedGraph.Edge;
import ru.yandex.cloud.ml.platform.lzy.graph.algo.DirectedGraph.Vertex;

public class Algorithms {

    public static <T extends Vertex> List<T> getNextBfsGroup(
        DirectedGraph<T> graph, List<T> currentGroup,
        Function<T, Boolean> canHaveChild
    ) {
        final Set<T> currentExecutions = new HashSet<>();
        final Set<T> next = new HashSet<>();
        final List<T> start = new ArrayList<>(currentGroup);

        for (T vertex: start) {
            if (canHaveChild.apply(vertex)) {
                currentExecutions.add(vertex);
            } else {
                next.add(vertex);
            }
        }

        for (T current: currentExecutions) {
            for (T vertex: graph.children(current.name())) {
                if (!next.contains(vertex)) {
                    if (
                        graph.parents(vertex.name())
                            .stream()
                            .allMatch(canHaveChild::apply)
                    ) {
                        next.add(vertex);
                    }
                }
            }
        }

        return next.stream().toList();
    }

    public static <T extends Vertex> Set<T> findRoots(DirectedGraph<T> graph) {
        Set<T> used = new HashSet<>();
        Set<T> roots = new HashSet<>();
        Queue<T> processingQueue = new ArrayDeque<>();
        for (T root: graph.vertexes()) {
            if (used.contains(root)) {
                continue;
            }
            processingQueue.add(root);
            used.add(root);
            while (!processingQueue.isEmpty()) {
                final T vertex = processingQueue.poll();
                if (graph.parents(vertex.name()).size() == 0) {
                    roots.add(vertex);
                    continue;
                }
                for (T connected: graph.parents(vertex.name())) {
                    if (!used.contains(connected)) {
                        used.add(connected);
                        processingQueue.add(connected);
                    }
                }
            }
        }
        return roots;
    }

    public static <T extends Vertex> CondensedGraph<T> condenseGraph(DirectedGraph<T> graph) {
        final Set<T> used = new HashSet<>();
        final List<T> order = new ArrayList<>();

        for (T vertex: graph.vertexes()) {
            if (!used.contains(vertex)) {
                topSort(vertex, order, used, graph);
            }
        }
        used.clear();
        Collections.reverse(order);

        final List<Set<T>> components = new ArrayList<>();

        for (T vertex: order) {
            if (!used.contains(vertex)) {
                final Set<T> component = new HashSet<>();
                findStrongConnections(vertex, component, used, graph);
                components.add(component);
            }
        }

        final Map<String, CondensedComponent<T>> vertexToComponentMapping = new HashMap<>();
        for (int i = 0; i < components.size(); i++) {
            for (Vertex vertex: components.get(i)) {
                vertexToComponentMapping.put(
                    vertex.name(),
                    new CondensedComponent<>(String.valueOf(i), components.get(i))
                );
            }
        }

        final CondensedGraph<T> condensedGraph = new CondensedGraph<>(vertexToComponentMapping);

        for (Vertex vertex: graph.vertexes()) {
            for (Vertex edge: graph.children(vertex.name())) {
                if (!vertexToComponentMapping.get(edge.name()).equals(vertexToComponentMapping.get(vertex.name()))) {
                    condensedGraph.addEdge(new Edge<>(
                        vertexToComponentMapping.get(vertex.name()),
                        vertexToComponentMapping.get(edge.name())
                    ));
                }
            }
        }
        return condensedGraph;
    }

    private static <T extends Vertex> void topSort(T vertex, List<T> order, Set<T> used, DirectedGraph<T> graph) {
        used.add(vertex);
        for (T edge : graph.children(vertex.name())) {
            if (!used.contains(edge)) {
                topSort(edge, order, used, graph);
            }
        }
        order.add(vertex);
    }

    private static <T extends Vertex> void findStrongConnections(T vertex, Set<T> component,
                                                  Set<T> used, DirectedGraph<T> graph) {
        used.add(vertex);
        component.add(vertex);
        for (T edge : graph.parents(vertex.name())) {
            if (!used.contains(edge)) {
                findStrongConnections(edge, component, used, graph);
            }
        }
    }

    public static class CondensedGraph<T extends Vertex> extends DirectedGraph<CondensedComponent<T>> {
        final Map<String, CondensedComponent<T>> vertexNameToComponentMap;

        public CondensedGraph(Map<String, CondensedComponent<T>> vertexNameToComponentMap) {
            this.vertexNameToComponentMap = vertexNameToComponentMap;
        }

        public Map<String, CondensedComponent<T>> vertexNameToComponentMap() {
            return vertexNameToComponentMap;
        }
    }

    public static class CondensedComponent<T extends Vertex> extends Vertex {
        private final String name;
        private final Set<T> vertices;

        public CondensedComponent(String name, Set<T> vertices) {
            this.name = name;
            this.vertices = vertices;
        }

        @Override
        String name() {
            return name;
        }

        public Set<T> vertices() {
            return vertices;
        }
    }
}
