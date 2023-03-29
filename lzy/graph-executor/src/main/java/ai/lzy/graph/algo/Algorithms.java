package ai.lzy.graph.algo;

import java.util.*;

public class Algorithms {

    public static <T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>> Set<T> getNextBfsGroup(
        DirectedGraph<T, E> graph,
        List<T> currentGroup
    )
    {
        final Set<T> next = new HashSet<>();

        final Set<T> currentExecutions = new HashSet<>(currentGroup);

        for (T current : currentExecutions) {
            for (E edge : graph.children(current.name())) {
                final T vertex = edge.output();
                next.add(vertex);
            }
        }

        return next;
    }

    public static <T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>> Set<T> findRoots(
        DirectedGraph<T, E> graph
    )
    {
        Set<T> used = new HashSet<>();
        Set<T> roots = new HashSet<>();
        Queue<T> processingQueue = new ArrayDeque<>();
        for (T root : graph.vertexes()) {
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
                for (E connected : graph.parents(vertex.name())) {
                    if (!used.contains(connected.input())) {
                        used.add(connected.input());
                        processingQueue.add(connected.input());
                    }
                }
            }
        }
        return roots;
    }

    public static <T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>> CondensedGraph<T, E> condenseGraph(
        DirectedGraph<T, E> graph
    )
    {
        final Set<T> used = new HashSet<>();
        final List<T> order = new ArrayList<>();

        for (T vertex : graph.vertexes()) {
            if (!used.contains(vertex)) {
                topSort(vertex, order, used, graph);
            }
        }
        used.clear();
        Collections.reverse(order);

        final List<Set<T>> components = new ArrayList<>();

        for (T vertex : order) {
            if (!used.contains(vertex)) {
                final Set<T> component = new HashSet<>();
                findStrongConnections(vertex, component, used, graph);
                components.add(component);
            }
        }

        final Map<String, CondensedComponent<T>> vertexToComponentMapping = new HashMap<>();
        for (int i = 0; i < components.size(); i++) {
            for (DirectedGraph.Vertex vertex : components.get(i)) {
                vertexToComponentMapping.put(
                    vertex.name(),
                    new CondensedComponent<>(String.valueOf(i), components.get(i))
                );
            }
        }

        final CondensedGraph<T, E> condensedGraph = new CondensedGraph<>(vertexToComponentMapping);
        final Map<EdgeKey, CondensedEdge<T, E>> edges = new HashMap<>();

        for (T input : graph.vertexes()) {
            condensedGraph.addVertex(vertexToComponentMapping.get(input.name()));

            for (E edge : graph.children(input.name())) {
                final T output = edge.output();
                final CondensedComponent<T> inputComponent = vertexToComponentMapping.get(input.name());
                final CondensedComponent<T> outputComponent = vertexToComponentMapping.get(output.name());

                if (!inputComponent.equals(outputComponent)) {

                    final CondensedEdge<T, E> condensedEdge = edges.computeIfAbsent(
                        new EdgeKey(inputComponent.name(), outputComponent.name()),
                        t -> new CondensedEdge<>(inputComponent, outputComponent));
                    condensedEdge.addEdge(edge);
                }
            }
        }

        condensedGraph.addEdges(edges.values());
        return condensedGraph;
    }

    private static <T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>> void topSort(
        T vertex,
        List<T> order,
        Set<T> used,
        DirectedGraph<T, E> graph
    )
    {
        used.add(vertex);
        for (E edge : graph.children(vertex.name())) {
            T v = edge.output();
            if (!used.contains(v)) {
                topSort(v, order, used, graph);
            }
        }
        order.add(vertex);
    }

    private static <T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>> void findStrongConnections(
        T vertex,
        Set<T> component,
        Set<T> used,
        DirectedGraph<T, E> graph
    )
    {
        used.add(vertex);
        component.add(vertex);
        for (E edge : graph.parents(vertex.name())) {
            T v = edge.input();
            if (!used.contains(v)) {
                findStrongConnections(v, component, used, graph);
            }
        }
    }

    public static class CondensedGraph<T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>>
        extends DirectedGraph<CondensedComponent<T>, CondensedEdge<T, E>>
    {
        final Map<String, CondensedComponent<T>> vertexNameToComponentMap;

        public CondensedGraph(Map<String, CondensedComponent<T>> vertexNameToComponentMap) {
            this.vertexNameToComponentMap = vertexNameToComponentMap;
        }

        public Map<String, CondensedComponent<T>> vertexNameToComponentMap() {
            return vertexNameToComponentMap;
        }
    }

    public static class CondensedComponent<T extends DirectedGraph.Vertex> extends DirectedGraph.Vertex {
        private final String name;
        private final Set<T> vertices;

        public CondensedComponent(String name, Set<T> vertices) {
            this.name = name;
            this.vertices = vertices;
        }

        @Override
        public String name() {
            return name;
        }

        public Set<T> vertices() {
            return vertices;
        }
    }

    public static class CondensedEdge<T extends DirectedGraph.Vertex, E extends DirectedGraph.Edge<T>> extends
        DirectedGraph.Edge<CondensedComponent<T>>
    {
        private final CondensedComponent<T> input;
        private final CondensedComponent<T> output;

        private final List<E> condensedEdges = new ArrayList<>();

        public CondensedEdge(CondensedComponent<T> input,
                             CondensedComponent<T> output)
        {
            this.input = input;
            this.output = output;
        }

        @Override
        public CondensedComponent<T> input() {
            return input;
        }

        @Override
        public CondensedComponent<T> output() {
            return output;
        }

        public List<E> condensedEdges() {
            return condensedEdges;
        }

        private void addEdge(E edge) {
            condensedEdges.add(edge);
        }
    }

    private static record EdgeKey(String input, String output) {
    }
}
