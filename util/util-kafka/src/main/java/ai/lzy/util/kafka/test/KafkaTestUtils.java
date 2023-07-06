package ai.lzy.util.kafka.test;

import ai.lzy.util.kafka.KafkaConfig;
import ai.lzy.util.kafka.KafkaHelper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public class KafkaTestUtils {

    public record EosMessage(String taskId, String stream) {}

    public record StdlogMessage(String taskId, String stream, String line) {
        public static StdlogMessage out(String taskId, String line) {
            return new StdlogMessage(taskId, "out", line);
        }
        public static StdlogMessage err(String taskId, String line) {
            return new StdlogMessage(taskId, "err", line);
        }
    }

    public static final class ReadKafkaTopicFinisher {
        private volatile boolean finish = false;
        private volatile boolean consumerFinished = false;

        public boolean shouldFinish() {
            return finish;
        }

        public synchronized void finish() {
            System.out.println(" --> finish kafka topic reader...");
            if (finish) {
                return;
            }
            finish = true;
            while (!consumerFinished) {
                LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            }
        }

        void consumerFinished() {
            consumerFinished = true;
        }
    }

    /**
     * @return Exception on error
     *         EosMessage on EOS
     *         StdlogMessage for each output line
     */
    public static ArrayBlockingQueue<Object> readKafkaTopic(String bootstrapServer, String topicName,
                                                            ReadKafkaTopicFinisher finisher)
    {
        var values = new ArrayBlockingQueue<>(100);

        var props = new KafkaHelper(KafkaConfig.of(bootstrapServer)).toProperties();
        props.put("group.id", "kafka-test-" + UUID.randomUUID());

        var thread = new Thread(() -> {
            try (var consumer = new KafkaConsumer<String, byte[]>(props)) {
                var partition = new TopicPartition(topicName, /* partition */ 0);

                consumer.assign(List.of(partition));
                consumer.seek(partition, 0);

                var ts = System.currentTimeMillis();

                while (!finisher.shouldFinish()) {
                    var records = consumer.poll(Duration.ofMillis(100));
                    if (records.count() <= 0) {
                        var now = System.currentTimeMillis();
                        if (ts - now > 5000) {
                            System.out.println("... waiting for data at topic " + topicName);
                            ts = now;
                        }
                        continue;
                    }

                    ts = System.currentTimeMillis();

                    // consumer.commitSync();

                    for (var record : records) {
                        var taskId = record.key();
                        var stream = new String(record.headers().lastHeader("stream").value(), StandardCharsets.UTF_8);

                        var eos = record.headers().lastHeader("eos") != null;
                        if (eos) {
                            var msg = new EosMessage(taskId, stream);
                            System.out.println(" ::: got " + msg);
                            values.offer(msg);
                            continue;
                        }

                        var lines = new String(record.value(), StandardCharsets.UTF_8);
                        for (var line : lines.split("\n")) {
                            var msg = new StdlogMessage(taskId, stream, line);
                            System.out.println(" ::: got " + msg);
                            values.offer(msg);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.printf("Cannot read from topic %s: %s%n", topicName, e.getMessage());
                values.offer(e);
            } finally {
                finisher.consumerFinished();
            }
        });
        thread.start();

        return values;
    }

    public static void assertStdLogs(BlockingQueue<Object> logs, List<StdlogMessage> stdout, List<StdlogMessage> stderr)
        throws InterruptedException
    {
        var expectedStdout = new HashMap<String, Queue<StdlogMessage>>();
        var expectedStderr = new HashMap<String, Queue<StdlogMessage>>();
        var eosStdout = new HashSet<String>();
        var eosStderr = new HashSet<String>();

        record TaskStream(String taskId, String stream) {}

        var notFinishedStreams = new HashSet<TaskStream>();

        for (var out : stdout) {
            expectedStdout.computeIfAbsent(out.taskId, __ -> new ArrayDeque<>()).add(out);
            notFinishedStreams.add(new TaskStream(out.taskId, "out"));
            notFinishedStreams.add(new TaskStream(out.taskId, "err"));
        }

        for (var err : stderr) {
            expectedStderr.computeIfAbsent(err.taskId, __ -> new ArrayDeque<>()).add(err);
            notFinishedStreams.add(new TaskStream(err.taskId, "out"));
            notFinishedStreams.add(new TaskStream(err.taskId, "err"));
        }

        System.out.println(" --> waiting for streams: " +
            notFinishedStreams.stream().map(Objects::toString).collect(Collectors.joining(",")));

        while (!notFinishedStreams.isEmpty()) {
            var log = logs.take();
            if (log instanceof StdlogMessage msg) {
                var remains = "out".equals(msg.stream)
                    ? expectedStdout.get(msg.taskId)
                    : expectedStderr.get(msg.taskId);

                Objects.requireNonNull(remains);
                if (remains.isEmpty()) {
                    throw new AssertionError();
                }
                var last = remains.remove();
                if (!Objects.equals(last, msg)) {
                    throw new AssertionError("%s != %s".formatted(last, msg));
                }
            } else if (log instanceof EosMessage msg) {
                var set = "out".equals(msg.stream) ? eosStdout : eosStderr;
                if (!set.add(msg.taskId)) {
                    throw new AssertionError();
                }

                var remains = "out".equals(msg.stream)
                    ? expectedStdout.get(msg.taskId)
                    : expectedStderr.get(msg.taskId);

                if (!(remains == null || remains.isEmpty())) {
                    throw new AssertionError();
                }
                notFinishedStreams.remove(new TaskStream(msg.taskId, msg.stream));

                System.out.println(" --> waiting for streams: " +
                    notFinishedStreams.stream().map(Objects::toString).collect(Collectors.joining(",")));
            } else {
                throw new AssertionError(log.toString());
            }
        }
    }
}
