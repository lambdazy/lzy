package ai.lzy.worker;

import ai.lzy.env.logs.LogStream;
import ai.lzy.env.logs.LogStreamCollection;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class LogStreams extends LogStreamCollection {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String LOG_PATTERN = "[SYS] %s %s - %s";

    public final LogStream stdout = this.stream("out");
    public final LogStream stderr = this.stream("err");

    // TODO: support separate system stream
    public final LogStream systemInfo = this.stream("err", /*formatter*/ s -> LOG_PATTERN.formatted(
        "[INFO]",
        DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneOffset.UTC)),
        s)
    );

    public final LogStream systemErr = this.stream("err", /*formatter*/ s -> LOG_PATTERN.formatted(
        "[ERROR]",
        DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneOffset.UTC)),
        s)
    );
}
