package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

class DataFlowGraph {
    record Node(
        String operationName,
        List<SlotDescription> inputs,
        List<SlotDescription> outputs
    ) {}

    private static final String edge = " -> ";

    private final List<List<Integer>> graph;
    private final ArrayList<Node> operations;

    // slot uri ---> (slot name, operation id)
    private final Map<String, Map.Entry<String, Integer>> dataSuppliers;
    private final Map<String, List<Map.Entry<String, Integer>>> dataConsumers;

    // slot uri ---> slots name
    private final Map<String, List<String>> danglingInputSlots;

    private List<Integer> cycle = null;

    public DataFlowGraph(Collection<Node> operations) {
        this.operations = new ArrayList<>(operations);

        dataSuppliers = new HashMap<>();
        dataConsumers = new HashMap<>();

        var i = 0;
        for (var operation : this.operations) {
            for (SlotDescription slot : operation.inputs) {
                var consumers = dataConsumers.computeIfAbsent(slot.getStorageUri(), k -> new ArrayList<>());
                consumers.add(new AbstractMap.SimpleImmutableEntry<>(slot.getPath(), i));
            }
            for (SlotDescription slot : operation.outputs) {
                var supplier = dataSuppliers.get(slot.getStorageUri());
                if (supplier != null) {
                    throw new RuntimeException("Output slot with uri '" + slot.getStorageUri() + "' already exists");
                }
                dataSuppliers.put(slot.getStorageUri(), new AbstractMap.SimpleEntry<>(slot.getPath(), i));
            }
            i++;
        }

        danglingInputSlots = new HashMap<>();
        graph = new ArrayList<>(operations.size());

        for (var operation : this.operations) {
            for (SlotDescription slot : operation.inputs) {
                if (!dataSuppliers.containsKey(slot.getStorageUri())) {
                    danglingInputSlots.compute(slot.getStorageUri(), (slotUri, consumers) -> {
                        consumers = (consumers != null) ? consumers : new ArrayList<>();
                        consumers.add(slot.getPath());
                        return consumers;
                    });
                }
            }

            var to = new ArrayList<Integer>();
            for (SlotDescription slot : operation.outputs) {
                var consumers = dataConsumers.get(slot.getStorageUri());
                if (consumers != null) {
                    for (var consumer : consumers) {
                        to.add(consumer.getValue());
                    }
                }
            }

            graph.add(to);
        }
    }

    /**
     * Returns map in which key is a slot uri and value is a list of associated input slots names
     *
     * @return -- slot uri ---> slot names
     */
    public Map<String, List<String>> getDanglingInputSlots() {
        return danglingInputSlots;
    }

    private int[] dfs(int v, int[] colors, int[] prev) {
        colors[v] = 1;

        for (var u : graph.get(v)) {
            if (colors[u] == 0) {
                // not visited yet
                prev[u] = v;
                int[] cycleEnds = dfs(u, colors, prev);
                if (cycleEnds != null) {
                    return cycleEnds;
                }
            } else if (colors[u] == 1) {
                // cycle found
                return new int[] {u, v};
            }
        }

        colors[v] = 2;
        return null;
    }

    public boolean hasCycle() {
        var n = operations.size();
        var colors = new int[n];
        var prev = new int[n];

        int[] cycleEnds = null;
        for (var i = 0; i < n; i++) {
            if (colors[i] == 0) {
                cycleEnds = dfs(i, colors, prev);
            }
            if (cycleEnds != null) {
                break;
            }
        }

        if (cycleEnds != null) {
            // fill the cycle
            var cycle = new LinkedList<Integer>();
            cycle.add(cycleEnds[0]);
            for (var v = cycleEnds[1]; v != cycleEnds[0]; v = prev[v]) {
                cycle.addFirst(v);
            }
            cycle.addFirst(cycleEnds[0]);

            this.cycle = cycle;
            return true;
        }

        return false;
    }

    public String printCycle() {
        if (cycle == null) {
            throw new IllegalStateException("Cycle not found");
        }
        return cycle.stream().map(i -> operations.get(i).operationName).collect(Collectors.joining(edge));
    }

    @Override
    public String toString() {
        // prints operations graph in dot notation

        List<Data> dataflow = getDataFlow();
        var stringBuilder = new StringBuilder("digraph {");

        for (var data : dataflow) {
            var slotUri = data.slotUri;
            var in = findSupplierOperationIdBy(slotUri);

            if (data.consumers != null) {
                for (String outSlotName : data.consumers) {
                    var out = findConsumerOperationIdBy(slotUri, outSlotName);
                    stringBuilder
                        .append("\t")
                        .append('"').append(in).append('"')
                        .append(edge)
                        .append('"').append(out).append('"')
                        .append(";");
                }
            } else {
                stringBuilder
                    .append("\t")
                    .append('"').append(in).append('"')
                    .append(";");
            }
        }

        stringBuilder.append("\n}");
        return stringBuilder.toString();
    }

    private String findSupplierOperationIdBy(String slotUri) {
        if (danglingInputSlots.containsKey(slotUri)) {
            return "portal";
        }

        int operationId = dataSuppliers.get(slotUri).getValue();
        return operations.get(operationId).operationName;
    }

    private String findConsumerOperationIdBy(String slotUri, String slotName) {
        List<Map.Entry<String, Integer>> consumersSlotNames = dataConsumers.get(slotUri);
        Map.Entry<String, Integer> slotNameAndOpId = consumersSlotNames.stream()
            .filter(slot -> slot.getKey().contentEquals(slotName))
            .findFirst()
            .orElseThrow();
        return operations.get(slotNameAndOpId.getValue()).operationName;
    }

    /**
     * Returns data which transfer through channel from some output slot. The data are identified by slot uri.
     *
     * @param slotUri   -- uri which identifies data from some output slot
     * @param supplier  -- the output slot name, null if data do not present in output
     * @param consumers -- names of input slots which consumes data by slot uri
     */
    public record Data(String slotUri, @Nullable String supplier, @Nullable List<String> consumers) {}

    public List<Data> getDataFlow() {
        var dataFromOutput = dataSuppliers.entrySet().parallelStream()
            .map(pair -> {
                var supplierSlotUri = pair.getKey();
                var supplierSlotName = pair.getValue().getKey();
                var consumerSlotNamesAndOpIds = dataConsumers.get(supplierSlotUri);

                List<String> consumerSlotNames = null;

                if (consumerSlotNamesAndOpIds != null) {
                    consumerSlotNames = new ArrayList<>(consumerSlotNamesAndOpIds.size());
                    for (var consumerSlotNamesAndOpId : consumerSlotNamesAndOpIds) {
                        consumerSlotNames.add(consumerSlotNamesAndOpId.getKey());
                    }
                }

                return new Data(supplierSlotUri, supplierSlotName, consumerSlotNames);
            })
            .toList();

        var notFoundInOutput = danglingInputSlots.entrySet().parallelStream()
            .map(pair -> new Data(pair.getKey(), null, pair.getValue()))
            .toList();

        return new ArrayList<>() {
            {
                addAll(dataFromOutput);
                addAll(notFoundInOutput);
            }
        };
    }
}