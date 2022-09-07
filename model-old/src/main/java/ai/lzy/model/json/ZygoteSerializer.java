package ai.lzy.model.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.protobuf.util.JsonFormat;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.Zygote;

import java.io.IOException;

public class ZygoteSerializer extends StdSerializer<Zygote> {
    public ZygoteSerializer() {
        this(null);
    }

    protected ZygoteSerializer(Class<Zygote> t) {
        super(t);
    }

    @Override
    public void serialize(Zygote message, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider) throws IOException {
        String s = JsonFormat.printer().print(GrpcConverter.to(message));
        jsonGenerator.writeRawValue(s);
    }
}
