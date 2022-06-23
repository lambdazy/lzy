package ai.lzy.model.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.protobuf.util.JsonFormat;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.Zygote;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.io.IOException;

public class ZygoteDeserializer extends StdDeserializer<Zygote> {
    protected ZygoteDeserializer(Class<?> vc) {
        super(vc);
    }

    public ZygoteDeserializer() {
        this(null);
    }

    @Override
    public Zygote deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        String s = jsonParser.readValueAsTree().toString();
        Operations.Zygote.Builder z = Operations.Zygote.newBuilder();
        JsonFormat.parser().merge(s, z);
        return GrpcConverter.from(z.build());
    }
}
