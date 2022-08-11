package ai.lzy.allocator.model;

import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;

import java.util.List;
import java.util.Map;

public record Workload(
    String name,
    String image,
    Map<String, String> env,
    List<String> args,
    Map<Integer, Integer> portBindings
) {
    public static Workload fromGrpc(AllocateRequest.Workload workload) {
        return new Workload(workload.getName(), workload.getImage(), workload.getEnvMap(),
            workload.getArgsList(), workload.getPortBindingsMap());
    }
}
