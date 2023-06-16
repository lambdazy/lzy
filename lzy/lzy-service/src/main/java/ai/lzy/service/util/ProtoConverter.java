package ai.lzy.service.util;

import ai.lzy.v1.VmPoolServiceApi;
import ai.lzy.v1.workflow.LWF;

public enum ProtoConverter {
    ;

    public static LWF.VmPoolSpec to(VmPoolServiceApi.VmPoolSpec pool) {
        return LWF.VmPoolSpec.newBuilder()
            .setPoolSpecName(pool.getLabel())
            .setCpuCount(pool.getCpuCount())
            .setGpuCount(pool.getGpuCount())
            .setRamGb(pool.getRamGb())
            .setCpuType(pool.getCpuType())
            .setGpuType(pool.getGpuType())
            .addAllZones(pool.getZonesList())
            .build();
    }
}
