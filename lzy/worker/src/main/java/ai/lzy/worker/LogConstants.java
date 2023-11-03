package ai.lzy.worker;

import ai.lzy.env.logs.LogStream;
import ai.lzy.env.logs.LogStreamCollection;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class LogConstants {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String LOG_PATTERN = "[SYS] %s - %s";

    public static LogStreamCollection LOGS = new LogStreamCollection();

    public static LogStream STDOUT = LOGS.stream("out");
    public static LogStream STDERR = LOGS.stream("err");

    // TODO: support separate system stream
    public static LogStream SYSTEM = LOGS.stream("err", /*formatter*/ s -> LOG_PATTERN.formatted(
        DATE_TIME_FORMATTER.format(LocalDateTime.now(ZoneOffset.UTC)),
        s)
    );
}
