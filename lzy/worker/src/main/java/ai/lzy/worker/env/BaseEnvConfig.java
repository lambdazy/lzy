package ai.lzy.worker.env;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class BaseEnvConfig {

    private final String image;
    private final Set<MountDescription> mounts;
    private final boolean needGpu;

    private BaseEnvConfig(String image, Set<MountDescription> mounts, boolean needGpu) {
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
        return this.mounts.stream().toList();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseEnvConfig that)) {
            return false;
        }

        if (needGpu != that.needGpu) {
            return false;
        }
        if (!Objects.equals(image, that.image)) {
            return false;
        }
        return mounts.equals(that.mounts);
    }

    @Override
    public int hashCode() {
        int result = image != null ? image.hashCode() : 0;
        result = 31 * result + mounts.hashCode();
        result = 31 * result + (needGpu ? 1 : 0);
        return result;
    }

    public record MountDescription(String source, String target, boolean isRshared) { }

    public static class Builder {

        String image = null;
        boolean gpu = false;
        Set<MountDescription> mounts = new HashSet<>();

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
