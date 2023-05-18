package ai.lzy.allocator.alloc.impl.kuber;

import io.fabric8.kubernetes.api.model.Node;

import java.util.Map;

public interface NodeController {
    Node getNode(String clusterId, String nodeName);

    void addLabels(String clusterId, String nodeName, Map<String, String> labels);
}
