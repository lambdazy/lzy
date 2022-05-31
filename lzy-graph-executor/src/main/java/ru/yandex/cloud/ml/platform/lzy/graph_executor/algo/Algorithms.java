package ru.yandex.cloud.ml.platform.lzy.graph_executor.algo;

import java.util.*;
import java.util.function.Function;

public class Algorithms {

    public static List<String> getNextBfsGroup(
        Graph graph,
        List<String> currentGroup,
        Function<String, Boolean> canHaveChild
        ) {
        final Set<String> currentExecutions = new HashSet<>();
        final Set<String> next = new HashSet<>();
        final List<String> start = new ArrayList<>(currentGroup);
        if (start.size() == 0) {
            start.addAll(findRoots(graph));
        }

        for (String vertex: start) {
            if (canHaveChild.apply(vertex)) {
                currentExecutions.add(vertex);
            } else {
                next.add(vertex);
            }
        }

        for (String current: currentExecutions) {
            for (String vertex: graph.children(current)) {
                if (!next.contains(vertex)) {
                    if (graph.parents(vertex).stream().allMatch(canHaveChild::apply)) {
                        next.add(vertex);
                    }
                }
            }
        }

        return next.stream().toList();
    }

    public static Set<String> findRoots(Graph graph) {
        Set<String> used = new HashSet<>();
        Set<String> roots = new HashSet<>();
        Queue<String> processingQueue = new ArrayDeque<>();
        for (String root: graph.vertexes()) {
            if (used.contains(root)) {
                continue;
            }
            processingQueue.add(root);
            used.add(root);
            while (!processingQueue.isEmpty()) {
                final String vertex = processingQueue.poll();
                if (graph.parents(vertex).size() == 0) {
                    roots.add(vertex);
                    continue;
                }
                for (String connected: graph.parents(vertex)) {
                    if (!used.contains(connected)) {
                        used.add(connected);
                        processingQueue.add(connected);
                    }
                }
            }
        }
        return roots;
    }

    public static CondensedGraph condenseGraph(Graph graph) {
        final Set<String> used = new HashSet<>();
        final List<String> order = new ArrayList<>();

        for (String vertex: graph.vertexes()) {
            if (!used.contains(vertex)) {
                topSort(vertex, order, used, graph);
            }
        }
        used.clear();
        Collections.reverse(order);

        final List<List<String>> components = new ArrayList<>();

        for (String vertex: order) {
            if (!used.contains(vertex)) {
                final ArrayList<String> component = new ArrayList<>();
                findStrongConnections(vertex, component, used, graph);
                components.add(component);
            }
        }

        final Graph condensedGraph = new Graph();
        final Map<String, String> vertexToComponentMapping = new HashMap<>();
        for (int i = 0; i < components.size(); i++) {
            for (String vertex: components.get(i)) {
                vertexToComponentMapping.put(vertex, String.valueOf(i));
            }
        }
        for (String vertex: graph.vertexes()) {
            for (String edge: graph.children(vertex)) {
                if (!vertexToComponentMapping.get(edge).equals(vertexToComponentMapping.get(vertex))) {
                    condensedGraph.addEdge(new Graph.Edge(
                        vertexToComponentMapping.get(vertex),
                        vertexToComponentMapping.get(edge)
                    ));
                }
            }
        }

        return new CondensedGraph(condensedGraph, vertexToComponentMapping);
    }

    private static void topSort(String vertex, List<String> order, Set<String> used, Graph graph) {
        used.add(vertex);
        for (String edge : graph.children(vertex)) {
            if (!used.contains(edge)) {
                topSort(edge, order, used, graph);
            }
        }
        order.add(vertex);
    }

    private static void findStrongConnections(String vertex, List<String> component, Set<String> used, Graph graph) {
        used.add(vertex);
        component.add(vertex);
        for (String edge : graph.parents(vertex)) {
            if (!used.contains(edge)) {
                findStrongConnections(edge, component, used, graph);
            }
        }
    }

    public record CondensedGraph(Graph graph, Map<String, String> vertexToComponentMapping) {}
}
