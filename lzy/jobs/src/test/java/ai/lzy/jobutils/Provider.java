package ai.lzy.jobutils;

import ai.lzy.jobsutils.providers.JobProviderBase;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.inject.Singleton;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Singleton
public class Provider extends JobProviderBase<Provider.Data> {
    public static Consumer<Data> onExecute = (a) -> {};

    protected Provider() {
        super(Data.class);
    }

    @Override
    protected void execute(Data arg) {
        onExecute.accept(arg);
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record Data(String a, String b) {}
}
