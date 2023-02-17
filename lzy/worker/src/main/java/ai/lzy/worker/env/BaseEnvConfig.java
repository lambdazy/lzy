package ai.lzy.worker.env;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record BaseEnvConfig(
    String image,
    List<MountDescription> mounts,
    boolean needGpu,
    List<String> envVars  // In format <NAME>=<value>
) {

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BaseEnvConfig{" +
            "image='" + image + '\'' +
            ", needGpu=" + needGpu +
            ", mounts=[" + mounts.stream()
            .map(it -> it.source() + " -> " + it.target() + (it.isRshared() ? " (R_SHARED)" : ""))
            .collect(Collectors.joining(", ")) + "]" +
            ", envVars=[" + String.join(",", envVars) + "]" +
            '}';
    }

    public record MountDescription(String source, String target, boolean isRshared) { }

    public static class Builder {

        String image = null;
        boolean gpu = false;
        List<MountDescription> mounts = new ArrayList<>();
        List<String> envVars = new ArrayList<>();

        public Builder withImage(String name) {
            this.image = name;
            return this;
        }

        public Builder withGpu(boolean withGpu) {
            this.gpu = withGpu;
            return this;
        }

        public Builder addMount(String source, String target) {
            mounts.add(new MountDescription(source, target, false));
            return this;
        }

        public Builder addRsharedMount(String source, String target) {
            mounts.add(new MountDescription(source, target, true));
            return this;
        }

        public Builder setEnvVars(Map<String, String> envVars) {
            this.envVars = envVars.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .toList();
            return this;
        }

        public BaseEnvConfig build() {
            return new BaseEnvConfig(image, mounts, gpu, envVars);
        }

    }

}
