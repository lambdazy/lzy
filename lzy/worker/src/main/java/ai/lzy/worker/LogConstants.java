package ai.lzy.worker;

import ai.lzy.env.logs.LogStream;
import ai.lzy.env.logs.LogStreamCollection;

public class LogConstants {
    public static LogStreamCollection LOGS = new LogStreamCollection();

    public static LogStream STDOUT = LOGS.stream("out");
    public static LogStream STDERR = LOGS.stream("err");
}
