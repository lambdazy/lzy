package ai.lzy.model.deprecated;

import java.util.stream.Stream;

@Deprecated
public interface DataSnapshot extends Stream<DataPage> {
    DataStream root();

    long size();

    Comparable version();
}
