package ai.lzy.model;

import java.util.Map;

public record DataScheme(
    String dataFormat,
    String schemeFormat,
    String schemeContent,
    Map<String, String> metadata
) {
    public static final DataScheme PLAIN = new DataScheme("raw_file", "no_schema", "", Map.of());
}
