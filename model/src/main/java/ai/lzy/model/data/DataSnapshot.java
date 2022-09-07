package ai.lzy.model.data;

import java.util.stream.Stream;

public interface DataSnapshot extends Stream<DataPage> {
    DataStream root();

    long size();

    Comparable version();
}
