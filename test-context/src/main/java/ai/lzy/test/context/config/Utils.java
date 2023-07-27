package ai.lzy.test.context.config;

import java.util.List;
import java.util.Map;

public class Utils {
    public static List<String> asCmdArgs(Map<String, Object> namedArgs) {
        return namedArgs.entrySet().stream().map(e -> "-" + e.getKey() + "=" + e.getValue()).toList();
    }
}
