package ru.yandex.cloud.ml.platform.lzy.servant.env;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EnvConfig {
    List<MountDescription> mounts = new ArrayList<>();

    public static class MountDescription {
        public final String source;
        public final String target;

        public MountDescription(String source, String target) {
            this.source = source;
            this.target = target;
        }
    }
}
