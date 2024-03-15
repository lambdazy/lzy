package ai.lzy.longrunning.task;

import jakarta.annotation.Nullable;

import java.util.Map;

public final class ResolverUtils {
    private ResolverUtils() {
    }

    @Nullable
    public static String readString(Map<String, Object> meta, String key) {
        var obj = meta.get(key);
        if (obj instanceof String s) {
            return s;
        }
        return null;
    }
}
