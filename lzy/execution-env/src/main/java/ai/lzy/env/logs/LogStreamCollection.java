package ai.lzy.env.logs;

import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class LogStreamCollection {
    private final List<LogStream> streams = new ArrayList<>();

    public LogStream stream(String name) {
        var stream =  new LogStream(name, null);
        streams.add(stream);
        return stream;
    }

    public LogStream stream(String name, @Nullable Function<String, String> formatter) {
        var stream =  new LogStream(name, formatter);
        streams.add(stream);
        return stream;
    }

    List<LogStream> getStreams() {
        return streams;
    }
}
