package ai.lzy.service.util;

import java.util.Collection;
import java.util.HashSet;
import javax.annotation.Nullable;

public enum CollectionUtils {
    ;

    @Nullable
    public static <T> T findFirstDuplicate(Collection<T> collection) {
        var scanned = new HashSet<T>();
        for (var cur : collection) {
            if (scanned.contains(cur)) {
                return cur;
            }
            scanned.add(cur);
        }
        return null;
    }
}
