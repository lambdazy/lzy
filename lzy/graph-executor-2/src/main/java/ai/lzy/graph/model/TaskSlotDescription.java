package ai.lzy.graph.model;

import java.util.List;
import java.util.Map;

public record TaskSlotDescription(
    String name,
    String description,
    String poolLabel,
    String zone,
    String command,
    List<Slot> slots,
    Map<String, String> slotsToChannelsAssignments,
    KafkaTopicDescription stdLogsKafkaTopic
) {
    public record Slot(
        String name,
        Media media,
        Direction direction,
        String dataFormat,
        String schemeFormat,
        String schemeContent,
        Map<String, String> metadata
    ) {
        public enum Media {
            FILE, PIPE, ARG
        }

        public enum Direction {
            INPUT, OUTPUT
        }
    }

    public record KafkaTopicDescription(
        List<String> bootstrapServers,
        String username,
        // TODO: LZY-49
        String password,
        String topic
    ) {}
}
