package ai.lzy.allocator.alloc.impl.kuber;

import java.util.Map;

public interface NodeController {
    void addLabels(String clusterId, String nodeName, Map<String, String> labels);
}
