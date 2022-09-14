package ai.lzy.model;

import java.util.Map;

public record DataScheme(
    String dataFormat,
    String schemaFormat,
    String schemaContent,
    Map<String, String> metadata
) {
    public static final DataScheme PLAIN = new DataScheme("plain", "", "text", Map.of());
}
