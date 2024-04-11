package ai.lzy.env.base;

import com.github.dockerjava.core.DockerClientConfig;
import jakarta.annotation.Nullable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public record DockerEnvDescription(
    String name,
    String image,
    List<MountDescription> mounts,
    boolean needGpu,
    List<String> envVars,  // In format <NAME>=<value>
    @Nullable String networkMode,
    DockerClientConfig dockerClientConfig,
    String user,
    Set<String> allowedPlatforms, // In format os/arch like "linux/amd64". Empty means all are allowed
    @Nullable Long memLimitMb
) {

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BaseEnvConfig{" +
            "name='" + name + '\'' +
            ", image='" + image + '\'' +
            ", needGpu=" + needGpu +
            ", networkMode=" + networkMode +
            ", allowedPlatforms=" + String.join(", ", allowedPlatforms) +
            ", mounts=[" + mounts.stream()
                .map(it -> it.source() + " -> " + it.target() + (it.isRshared() ? " (R_SHARED)" : ""))
                .collect(Collectors.joining(", ")) + "]" +
            '}';
    }

    public record MountDescription(
        String source,
        String target,
        boolean isRshared
    ) {}

    public static class Builder {
        private static final String ROOT_USER_UID = "0";

        String name = null;
        String image = null;
        boolean gpu = false;
        List<MountDescription> mounts = new ArrayList<>();
        List<String> envVars = new ArrayList<>();
        String networkMode = null;
        DockerClientConfig dockerClientConfig;
        String user = ROOT_USER_UID;
        Set<String> allowedPlatforms = new HashSet<>();
        Long memLimitMb = null;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

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

        public Builder withEnvVars(Map<String, String> envVars) {
            envVars.forEach((key, value) -> this.envVars.add(key + "=" + value));
            return this;
        }

        public Builder withNetworkMode(@Nullable String mode) {
            networkMode = mode;
            return this;
        }

        public Builder withDockerClientConfig(DockerClientConfig dockerClientConfig) {
            this.dockerClientConfig = dockerClientConfig;
            return this;
        }

        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withAllowedPlatforms(Collection<String> allowedPlatforms) {
            this.allowedPlatforms.addAll(allowedPlatforms);
            return this;
        }

        public Builder withMemLimitMb(Long memLimitMb) {
            this.memLimitMb = memLimitMb;
            return this;
        }

        public DockerEnvDescription build() {
            if (StringUtils.isBlank(name)) {
                name = "job-" + RandomStringUtils.randomAlphanumeric(5);
            }
            return new DockerEnvDescription(name, image, mounts, gpu, envVars, networkMode, dockerClientConfig, user,
                allowedPlatforms, memLimitMb);
        }
    }

    public record ContainerRegistryCredentials(
        String url,
        String username,
        String password
    ) { }
}
