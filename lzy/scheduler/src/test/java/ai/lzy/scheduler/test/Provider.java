package ai.lzy.scheduler.test;

import ai.lzy.scheduler.JobService;
import ai.lzy.scheduler.providers.JobProviderBase;
import ai.lzy.scheduler.providers.JsonJobSerializer;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.inject.Singleton;

import java.util.function.Consumer;

@Singleton
public class Provider extends JobProviderBase<Provider.Data> {
    public static Consumer<Data> onExecute = (a) -> {};

    protected Provider(JobService jobService, DataSerializer serializer) {
        super(serializer, jobService, Data.class);
    }

    @Override
    protected void executeJob(Data arg) {
        onExecute.accept(arg);
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    @JsonSerialize
    @JsonDeserialize
    public record Data(String a, String b) {}

    @Singleton
    public static class DataSerializer extends JsonJobSerializer<Data> {
        protected DataSerializer() {
            super(Provider.Data.class);
        }
    }
}
