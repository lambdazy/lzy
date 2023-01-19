package ai.lzy.worker.env;

import java.util.ArrayList;
import java.util.List;

public class BaseEnvConfig {

    private final String image;
    private final List<MountDescription> mounts;

    private BaseEnvConfig(String image, List<MountDescription> mounts) {
        this.image = image;
        this.mounts = mounts;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String image() {
        return this.image;
    }

    public List<MountDescription> mounts() {
        return this.mounts;
    }

    public record MountDescription(String source, String target, boolean isRshared) { }

    public static class Builder {

        String image = null;
        List<MountDescription> mounts = new ArrayList<>();

        public Builder image(String name) {
            this.image = name;
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
            return new BaseEnvConfig(image, mounts);
        }

    }

}
