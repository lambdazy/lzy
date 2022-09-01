package ai.lzy.allocator.model;

import ai.lzy.allocator.volume.VolumeMount;
import ai.lzy.v1.VmAllocatorApi.AllocateRequest;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Workload(
    String name,
    String image,
    Map<String, String> env,
    List<String> args,
    Map<Integer, Integer> portBindings,
    List<VolumeMount> mounts
) {
    public static Workload fromProto(AllocateRequest.Workload workload) {
        return new Workload(workload.getName(), workload.getImage(), workload.getEnvMap(),
            workload.getArgsList(), workload.getPortBindingsMap(),
            workload.getVolumeMountsList()
                .stream()
                .map(m -> new VolumeMount(
                    m.getVolumeName(),
                    m.getMountPath(),
                    m.getReadOnly(),
                    VolumeMount.MountPropagation.valueOf(m.getMountPropagation().name()))
                ).collect(Collectors.toList()));
    }
}
