package ai.lzy.model.data;

import java.util.stream.Stream;

public interface DataStream extends Stream<DataPage> {
    DataSnapshot snapshot();

    Comparable version();
}
