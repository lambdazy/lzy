package ai.lzy.model.deprecated;

import ai.lzy.model.DataScheme;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Deprecated
public interface DataPage {
    URI id();

    List<Item> contents();

    DataScheme schema();

    interface Item extends Map<String, Object> {
        UUID id();

        Item[] dependsOn();

        DataPage page();
    }
}
