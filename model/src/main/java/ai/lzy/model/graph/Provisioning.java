package ai.lzy.model.graph;

import java.util.Set;
import java.util.stream.Stream;

public interface Provisioning {
    Set<String> tags();

    class Any implements Provisioning {
        @Override
        public Set<String> tags() {
            return Set.of();
        }
    }
}
