package ai.lzy.model.data;

public record DataSchema(SchemeType schemeType, String typeContent) {
    public static final DataSchema plain = new DataSchema(SchemeType.plain, "default");

    public static DataSchema buildDataSchema(String dataSchemeType, String typeContent) {
        return new DataSchema(SchemeType.valueOf(dataSchemeType), typeContent);
    }
}
