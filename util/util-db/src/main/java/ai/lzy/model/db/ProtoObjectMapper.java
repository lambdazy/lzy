package ai.lzy.model.db;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;

public class ProtoObjectMapper extends ObjectMapper {
    public ProtoObjectMapper() {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature());
        disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        setSerializationInclusion(JsonInclude.Include.NON_NULL);

        registerModule(new ProtobufModule());
    }
}