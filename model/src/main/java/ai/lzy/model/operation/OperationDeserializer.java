package ai.lzy.model.operation;

import ai.lzy.v1.common.LMO;
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
        LMO.Operation.Builder opBuilder = LMO.Operation.newBuilder();
        JsonFormat.parser().merge(s, opBuilder);
        return Operation.fromProto(opBuilder.build());
    }
}
