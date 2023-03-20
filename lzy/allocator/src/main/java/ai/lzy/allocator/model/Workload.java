package ai.lzy.allocator.model;

import ai.lzy.v1.VmAllocatorApi.AllocateRequest;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonSerialize
@JsonDeserialize
public record Workload(
    @JsonInclude
    String name,
    @JsonInclude
    String image,
    @JsonInclude
    Map<String, String> env,
    @JsonInclude
    List<String> args,
    @JsonInclude
    Map<Integer, Integer> portBindings,
    @JsonInclude
    Integer runAsUser,
    @JsonInclude
    List<VolumeMount> mounts
) {
    public static Workload fromProto(AllocateRequest.Workload workload) {
        return new Workload(
            workload.getName(),
            workload.getImage(),
            workload.getEnvMap(),
            workload.getArgsList(),
            workload.getPortBindingsMap(),
            workload.getRunAsUser(),
            workload.getVolumeMountsList().stream()
                .map(m -> new VolumeMount(
                    m.getVolumeName(),
                    m.getMountPath(),
                    m.getReadOnly(),
                    VolumeMount.MountPropagation.valueOf(m.getMountPropagation().name())))
                .toList()
        );
    }

    public static Workload fromProto(AllocateRequest.Workload workload, Map<String, String> extraEnv) {
        var env = new HashMap<>(workload.getEnvMap());
        env.putAll(extraEnv);

        return new Workload(
            workload.getName(),
            workload.getImage(),
            env,
            workload.getArgsList(),
            workload.getPortBindingsMap(),
            workload.getRunAsUser(),
            workload.getVolumeMountsList().stream()
                .map(m -> new VolumeMount(
                    m.getVolumeName(),
                    m.getMountPath(),
                    m.getReadOnly(),
                    VolumeMount.MountPropagation.valueOf(m.getMountPropagation().name())))
                .toList()
        );
    }

    public Workload withEnv(String key, String value) {
        final var env = new HashMap<>(this.env);
        env.put(key, value);

        return new Workload(name, image, env, args, portBindings, runAsUser, mounts);
    }
}
