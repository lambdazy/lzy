package ai.lzy.allocator.alloc.impl.kuber;

public interface NodeRemover {
    void removeNode(String vmId, String nodeName, String nodeInstanceId);
}
