package ai.lzy.env.base;

import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record DockerEnvDescription(
    String image,
    List<MountDescription> mounts,
    boolean needGpu,
    List<String> envVars,  // In format <NAME>=<value>
    @Nullable ContainerRegistryCredentials credentials  // Credentials to pull images with
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
            '}';
    }

    public record MountDescription(String source, String target, boolean isRshared, boolean isReadOnly) { }

    public static class Builder {

        String image = null;
        boolean gpu = false;
        List<MountDescription> mounts = new ArrayList<>();
        List<String> envVars = new ArrayList<>();
        ContainerRegistryCredentials credentials = null;

        public Builder withImage(String name) {
            this.image = name;
            return this;
        }

        public Builder withGpu(boolean withGpu) {
            this.gpu = withGpu;
            return this;
        }

        public Builder addMount(String source, String target, boolean readOnly) {
            mounts.add(new MountDescription(source, target, false, readOnly));
            return this;
        }

        public Builder addMount(String source, String target) {
            mounts.add(new MountDescription(source, target, false, false));
            return this;
        }

        public Builder addRsharedMount(String source, String target, boolean readOnly) {
            mounts.add(new MountDescription(source, target, true, readOnly));
            return this;
        }

        public Builder addRsharedMount(String source, String target) {
            mounts.add(new MountDescription(source, target, true, false));
            return this;
        }

        public Builder withEnvVars(Map<String, String> envVars) {
            envVars.forEach((key, value) -> this.envVars.add(key + "=" + value));
            return this;
        }

        public Builder withCredentials(ContainerRegistryCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        public DockerEnvDescription build() {
            return new DockerEnvDescription(image, mounts, gpu, envVars, credentials);
        }

    }

    public record ContainerRegistryCredentials(
        String url,
        String username,
        String password
    ) { }
}
