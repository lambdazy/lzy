package ai.lzy.model.deprecated;

import java.util.stream.Stream;

@Deprecated
public interface DataStream extends Stream<DataPage> {
    DataSnapshot snapshot();

    Comparable version();
}
