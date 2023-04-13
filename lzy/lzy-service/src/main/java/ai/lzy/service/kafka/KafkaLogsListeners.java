package ai.lzy.service.kafka;

import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.KafkaTopicDesc;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.workflow.LWFS.ReadStdSlotsRequest;
import ai.lzy.v1.workflow.LWFS.ReadStdSlotsResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class KafkaLogsListeners {
    private static final Logger LOG = LogManager.getLogger(KafkaLogsListeners.class);
    private static final byte[] STDOUT_STREAM_HEADER = "out".getBytes(StandardCharsets.UTF_8);

    private final ConcurrentHashMap<String, ArrayList<Listener>> listeners = new ConcurrentHashMap<>();
    @Nullable
    private final Properties kafkaSetup;

    public KafkaLogsListeners(LzyServiceConfig config) {
        if (config.getKafka().isEnabled()) {
            kafkaSetup = new KafkaHelper(config.getKafka()).toProperties();
        } else {
            kafkaSetup = null;
        }
    }

    public void listen(ReadStdSlotsRequest request, StreamObserver<ReadStdSlotsResponse> response,
                       KafkaTopicDesc topicDesc)
    {
        if (kafkaSetup == null) {
            return;
        }

        var listener = new Listener(topicDesc, request, response, Context.current());
        listener.start();
        // TODO: thread safe?
        listeners.computeIfAbsent(request.getExecutionId(), (k) -> new ArrayList<>()).add(listener);
    }

    public void notifyFinished(String executionId) {
        if (kafkaSetup == null) {
            return;
        }

        LOG.info("Finishing listeners for execution {}", executionId);
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
            super("kafka-logs-listener-%s".formatted(request.getExecutionId()));
            this.topicDesc = topicDesc;
            this.request = request;
            this.response = response;
            this.grpcContext = grpcContext;
        }

        @Override
        public void run() {
            try {
                GrpcHeaders.withContext(grpcContext.fork(), () -> {  // Fork context to get cancel from client

                    var props = (Properties) kafkaSetup.clone();
                    props.put("group.id", UUID.randomUUID().toString());

                    try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
                        var partition = new TopicPartition(topicDesc.topicName(), /* partition */ 0);

                        consumer.assign(List.of(partition));
                        consumer.seek(partition, request.getOffset());

                        var eos = false;

                        while (!Context.current().isCancelled()) {
                            if (finished.get() && eos) {
                                break;
                            }

                            var records = consumer.poll(Duration.ofMillis(100));
                            if (records.count() > 0) {
                                var outData = ReadStdSlotsResponse.Data.newBuilder();
                                var errData = ReadStdSlotsResponse.Data.newBuilder();
                                long offset = 0;

                                for (var record : records) {
                                    offset = Long.max(record.offset(), offset);

                                    var taskId = record.key();
                                    var stream = record.headers().lastHeader("stream");
                                    var data = ReadStdSlotsResponse.TaskLines.newBuilder()
                                        .setTaskId(taskId)
                                        .setLines(new String(record.value(), StandardCharsets.UTF_8))
                                        .build();

                                    if (stream == null || Arrays.equals(stream.value(), STDOUT_STREAM_HEADER)) {
                                        outData.addData(data);
                                    } else {
                                        errData.addData(data);
                                    }
                                }

                                var resp = ReadStdSlotsResponse.newBuilder()
                                    .setStderr(errData.build())
                                    .setStdout(outData.build())
                                    .setOffset(offset)
                                    .build();

                                response.onNext(resp);

                                consumer.commitSync();

                                eos = false;
                            } else {
                                eos = true;
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
