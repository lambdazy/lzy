package ai.lzy.env.base;

import com.github.dockerjava.core.DockerClientConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record DockerEnvDescription(
    String name,
    String image,
    List<MountDescription> mounts,
    boolean needGpu,
    List<String> envVars,  // In format <NAME>=<value>
    DockerClientConfig dockerClientConfig
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
        String name = null;
        String image = null;
        boolean gpu = false;
        List<MountDescription> mounts = new ArrayList<>();
        List<String> envVars = new ArrayList<>();
        DockerClientConfig dockerClientConfig;

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

        public Builder withDockerClientConfig(DockerClientConfig dockerClientConfig) {
            this.dockerClientConfig = dockerClientConfig;
            return this;
        }

        public DockerEnvDescription build() {
            if (StringUtils.isBlank(name)) {
                name = "job-" + RandomStringUtils.randomAlphanumeric(5);
            }
            return new DockerEnvDescription(name, image, mounts, gpu, envVars, dockerClientConfig);
        }

    }

    public record ContainerRegistryCredentials(
        String url,
        String username,
        String password
    ) { }
}
