package ai.lzy.disk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import jakarta.inject.Singleton;

@Singleton
public class ExtendedObjectMapper extends ObjectMapper {
    public ExtendedObjectMapper() {
        super();
        registerModule(new ProtobufModule());
    }
}
