package ai.lzy.test.impl.v2;

import ai.lzy.util.kafka.KafkaHelper;
import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import scala.collection.immutable.Map$;

@Singleton
public class KafkaContext {
    private final EmbeddedK kafka;
    private final String bootstrapServers;

    public KafkaContext() {
        scala.collection.immutable.Map<String, String> conf = Map$.MODULE$.empty();
        var config = EmbeddedKafkaConfig$.MODULE$.apply(
            8001,
            8002,
            conf,
            conf,
            conf
        );
        this.kafka = EmbeddedKafka.start(config);
        this.bootstrapServers = "localhost:8001";

        KafkaHelper.USE_AUTH.set(false);
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @PreDestroy
    public void close() {
        kafka.stop(true);
    }
}
