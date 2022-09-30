package ai.lzy.service.graph;

import ai.lzy.v1.workflow.LWF;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

public class DataFlowGraph {
    private final List<List<Integer>> graph;
    private final ArrayList<LWF.Operation> operations;

    // slot uri ---> (slot name, operation id)
    private final Map<String, Map.Entry<String, Integer>> dataSuppliers;
    private final Map<String, List<Map.Entry<String, Integer>>> dataConsumers;

    // slot uri ---> slots name
    private final Map<String, List<String>> danglingInputSlots;

    private List<Integer> cycle = null;

    public DataFlowGraph(Collection<LWF.Operation> operations) {
        this.operations = new ArrayList<>(operations);

        dataSuppliers = new HashMap<>();
        dataConsumers = new HashMap<>();

        var i = 0;
        for (var operation : this.operations) {
            for (LWF.Operation.SlotDescription slot : operation.getInputSlotsList()) {
                var consumers = dataConsumers.computeIfAbsent(slot.getStorageUri(), k -> new ArrayList<>());
                consumers.add(new AbstractMap.SimpleImmutableEntry<>(slot.getPath(), i));
            }
            for (LWF.Operation.SlotDescription slot : operation.getOutputSlotsList()) {
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
            for (LWF.Operation.SlotDescription slot : operation.getInputSlotsList()) {
                if (!dataSuppliers.containsKey(slot.getStorageUri())) {
                    danglingInputSlots.compute(slot.getStorageUri(), (slotUri, consumers) -> {
                        consumers = (consumers != null) ? consumers : new ArrayList<>();
                        consumers.add(slot.getPath());
                        return consumers;
                    });
                }
            }

            var to = new ArrayList<Integer>();
            for (LWF.Operation.SlotDescription slot : operation.getOutputSlotsList()) {
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

    public boolean findCycle() {
        var n = operations.size();
        var colors = new int[n];
        var prev = new int[n];

        int[] cycleEnds = null;
        for (var i = 0; i < n; i++) {
            if (colors[i] != 0) {
                cycleEnds = dfs(i, colors, prev);
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
        }

        return false;
    }

    public String printCycle() {
        if (cycle == null) {
            throw new IllegalStateException("Cycle not found");
        }
        return cycle.stream().map(i -> operations.get(i).getName()).collect(Collectors.joining(" --> "));
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
