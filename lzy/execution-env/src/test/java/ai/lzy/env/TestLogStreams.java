package ai.lzy.env;

import ai.lzy.env.logs.LogStream;
import ai.lzy.env.logs.LogWriter;
import ai.lzy.env.logs.Logs;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class TestLogStreams extends Logs {
    public final LogStream stdout = this.stream("STDOUT");
    public final LogStream stderr = this.stream("STDERR");

    public TestLogStreams() {
        init(List.of(new LogWriter() {
            @Override
            public void writeLines(String streamName, byte[] lines) {
                var stream = switch (streamName) {
                    case "STDOUT" -> System.out;
                    case "STDERR" -> System.err;
                    default -> throw new AssertionError("Unexpected stream '%s'".formatted(streamName));
                };
                stream.println(streamName + ": " + new String(lines, StandardCharsets.UTF_8));
            }

            @Override
            public void writeEos(String streamName) {
            }
        }));
    }
}
