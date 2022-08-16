package ai.lzy.model.json;

import ai.lzy.model.Operation;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;

public class OperationSerializer extends StdSerializer<Operation> {

    protected OperationSerializer() {
        super(Operation.class);
    }

    @Override
    public void serialize(Operation message, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
        String s = JsonFormat.printer().print(message.to());
        jsonGenerator.writeRawValue(s);
    }
}
