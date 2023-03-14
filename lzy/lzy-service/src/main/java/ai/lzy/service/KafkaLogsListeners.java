package ai.lzy.service;

import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LWFS.ReadStdSlotsRequest;
import ai.lzy.v1.workflow.LWFS.ReadStdSlotsResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class KafkaLogsListeners {
    private final LzyServiceConfig config;
    private final ConcurrentHashMap<String, ArrayList<Listener>> listeners = new ConcurrentHashMap<>();
    private static final Logger LOG = LogManager.getLogger(KafkaLogsListeners.class);

    @Inject
    public KafkaLogsListeners(LzyServiceConfig config) {
        this.config = config;
    }

    public void listen(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> response,
                       KafkaTopicDesc topicDesc)
    {
        var listener = new Listener(topicDesc, request, response, Context.current());
        listener.start();
        listeners.computeIfAbsent(request.getExecutionId(), (k) -> new ArrayList<>()).add(listener);
    }

    public void notifyFinished(String executionId) {
        for (var listener: listeners.computeIfAbsent(executionId, (k) -> new ArrayList<>())) {
            listener.close();
        }

        for (var listener: listeners.get(executionId)) {
            try {
                listener.join();
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    class Listener extends Thread {
        private final KafkaTopicDesc topicDesc;
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private final ReadStdSlotsRequest request;
        private final StreamObserver<ReadStdSlotsResponse> response;
        private final Context grpcContext;

        Listener(KafkaTopicDesc topicDesc, ReadStdSlotsRequest request,
                 StreamObserver<ReadStdSlotsResponse> response, Context grpcContext)
        {
            super("kafka-logs-listener");
            this.topicDesc = topicDesc;
            this.request = request;
            this.response = response;
            this.grpcContext = grpcContext;
        }

        @Override
        public void run() {
            try {
                GrpcHeaders.withContext(grpcContext.fork(), () -> {
                    var props = new Properties();
                    props.setProperty("bootstrap.servers", Strings.join(config.getKafka().getBootstrapServers(), ','));
                    props.setProperty("enable.auto.commit", "false");
                    props.setProperty("group.id", UUID.randomUUID().toString());
                    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    props.setProperty("value.deserializer",
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
                    props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "  username=\"" + config.getKafka().getUsername() + "\"" +
                        "  password=\"" + config.getKafka().getPassword() + "\";");

                    try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
                        consumer.assign(List.of(new TopicPartition(topicDesc.topicName(), 0)));

                        consumer.seek(new TopicPartition(topicDesc.topicName(), 0), request.getOffset());
                        var lastResultNotNull = true;

                        while (!Context.current().isCancelled()) {
                            if (finished.get() && !lastResultNotNull) {
                                break;
                            }

                            var res = consumer.poll(Duration.ofMillis(100));
                            if (res.count() > 0) {

                                var outData = ReadStdSlotsResponse.Data.newBuilder();
                                var errData = ReadStdSlotsResponse.Data.newBuilder();
                                long offset = 0;

                                for (var record : res) {
                                    offset = Long.max(record.offset(), offset);
                                    if (record.key().equals("out")) {
                                        outData.addData(new String(record.value(), StandardCharsets.UTF_8));
                                    } else {
                                        errData.addData(new String(record.value(), StandardCharsets.UTF_8));
                                    }
                                }

                                var resp = ReadStdSlotsResponse.newBuilder()
                                    .setStderr(errData.build())
                                    .setStdout(outData.build())
                                    .setCurrentOffset(offset)
                                    .build();

                                response.onNext(resp);

                                lastResultNotNull = true;
                            } else {
                                lastResultNotNull = false;
                            }
                        }
                    }
                    response.onCompleted();
                });
            } catch (Exception e) {
                LOG.error("Error while reading from kafka topic", e);
                response.onError(e);
            }
        }

        public void close() {
            finished.set(true);
        }
    }
}
