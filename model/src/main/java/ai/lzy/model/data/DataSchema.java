package ai.lzy.model.data;

import ai.lzy.model.data.types.SchemeType;

public record DataSchema(SchemeType schemeType, String typeContent) {
    public static final DataSchema plain = new DataSchema(SchemeType.plain, "");

    public static DataSchema buildDataSchema(String dataSchemeType, String typeContent) {
        return new DataSchema(SchemeType.valueOf(dataSchemeType), typeContent);
    }
}