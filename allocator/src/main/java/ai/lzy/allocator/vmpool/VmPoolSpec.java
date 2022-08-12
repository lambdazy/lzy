package ai.lzy.allocator.vmpool;

import ai.lzy.v1.VmPoolServiceApi;

import java.util.Set;

public record VmPoolSpec(
    String label,             // 's', 'm', 'l', ...
    String cpuType,           // IceLake, CascadeLake, Broadwell, ...
    int cpuCount,             // # of CPU cores
    String gpuType,           // V100, A100, ...
    int gpuCount,             // # of GPU cores
    int ramGb,                // RAM in GB
    Set<String> zones         // availability zones
) {

    public VmPoolServiceApi.VmPoolSpec toProto() {
        return VmPoolServiceApi.VmPoolSpec.newBuilder()
            .setLabel(label)
            .setCpuType(cpuType)
            .setCpuCount(cpuCount)
            .setGpuType(gpuType)
            .setGpuCount(gpuCount)
            .setRamGb(ramGb)
            .addAllZones(zones)
            .build();
    }
}
