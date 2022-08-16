package ai.lzy.model.json;

import ai.lzy.model.Operation;
import ai.lzy.v1.common.LzyCommon;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;

public class OperationDeserializer extends StdDeserializer<Operation> {
    protected OperationDeserializer() {
        super(Operation.class);
    }

    @Override
    public Operation deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
        String s = jsonParser.readValueAsTree().toString();
        LzyCommon.Operation.Builder z = LzyCommon.Operation.newBuilder();
        JsonFormat.parser().merge(s, z);
        return Operation.from(z.build());
    }
}
