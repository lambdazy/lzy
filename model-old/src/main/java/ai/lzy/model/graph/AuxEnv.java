package ai.lzy.model.graph;

import java.net.URI;

public interface AuxEnv {
    // TODO (d-kruchinin): Do we need to use String instead of URI here as it is in BaseEnv?
    URI uri();
}
