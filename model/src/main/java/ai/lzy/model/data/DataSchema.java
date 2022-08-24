package ai.lzy.model.data;

import ai.lzy.model.data.types.SchemeType;

public record DataSchema(SchemeType schemeType, String typeContent /* TODO @Nullable String serializerName */) {
    public static final DataSchema plain = new DataSchema(SchemeType.plain, "default");

    public static DataSchema buildDataSchema(String dataSchemeType, String typeContent) {
        return new DataSchema(SchemeType.valueOf(dataSchemeType), typeContent);
    }
}
