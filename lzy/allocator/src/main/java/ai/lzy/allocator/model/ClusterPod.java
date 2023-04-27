package ai.lzy.allocator.model;

public record ClusterPod(String clusterId, String podName) {
    public static ClusterPod of(String clusterId, String podName) {
        return new ClusterPod(clusterId, podName);
    }
}
