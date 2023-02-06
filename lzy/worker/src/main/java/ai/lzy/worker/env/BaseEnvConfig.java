package ai.lzy.worker.env;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BaseEnvConfig {

    private final String image;
    private final List<MountDescription> mounts;
    private final boolean needGpu;

    private BaseEnvConfig(String image, List<MountDescription> mounts, boolean needGpu) {
        this.image = image;
        this.mounts = mounts;
        this.needGpu = needGpu;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String image() {
        return this.image;
    }

    public boolean needGpu() {
        return this.needGpu;
    }

    public List<MountDescription> mounts() {
        return this.mounts;
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

    public record MountDescription(String source, String target, boolean isRshared) { }

    public static class Builder {

        String image = null;
        boolean gpu = false;
        List<MountDescription> mounts = new ArrayList<>();

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

        public BaseEnvConfig build() {
            return new BaseEnvConfig(image, mounts, gpu);
        }

    }

}
