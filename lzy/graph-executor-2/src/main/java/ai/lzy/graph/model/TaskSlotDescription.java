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
    String stdoutName,
    String stdoutChannelId,
    String stderrName,
    String stderrChannelId,
    KafkaTopicDescription kafkaTopicDescription
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
        enum Media {
            UNSPECIFIED, FILE, PIPE, ARG
        }

        enum Direction {
            UNKNOWN, INPUT, OUTPUT
        }
    }

    public record KafkaTopicDescription(
        List<String> bootstrapServers,
        String username,
        String password,
        String topic
    ) {}
}
