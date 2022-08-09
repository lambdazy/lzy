package ai.lzy.allocator.model;

import java.util.List;
import java.util.Map;

public record Workload(String name, String image, Map<String, String> env,
        List<String> args, Map<Integer, Integer> portBindings) {
}
