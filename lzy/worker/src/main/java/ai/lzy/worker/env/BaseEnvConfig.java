package ai.lzy.worker.env;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseEnvConfig {

    private final String image;
    private final List<MountDescription> mounts;

    private BaseEnvConfig(String image, Map<String, String> mounts) {
        this.image = image;
        this.mounts = new ArrayList<>();
        mounts.forEach((source, target) ->
            this.mounts.add(new MountDescription(source, target))
        );
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

    public static class MountDescription {
        public final String source;
        public final String target;

        public MountDescription(String source, String target) {
            this.source = source;
            this.target = target;
        }
    }

    public static class Builder {

        String image = null;
        Map<String, String> mounts = new HashMap<>();

        public Builder image(String name) {
            this.image = name;
            return this;
        }

        public Builder addMount(String source, String target) {
            this.mounts.put(source, target);
            return this;
        }

        public BaseEnvConfig build() {
            return new BaseEnvConfig(image, mounts);
        }

    }

}
