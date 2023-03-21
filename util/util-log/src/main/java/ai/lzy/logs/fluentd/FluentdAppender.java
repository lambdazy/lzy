package ai.lzy.logs.fluentd;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.fluentd.FluencyBuilderForFluentd;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.UUID;
import javax.annotation.Nullable;

@Plugin(name = FluentdAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE,
        printObject = true)
public class FluentdAppender extends AbstractAppender {

    public static final String PLUGIN_NAME = "FluentdAppender";
    private final Fluency fluency;

    public FluentdAppender(
        String name,
        String host,
        int port,
        Filter filter,
        @Nullable Property[] properties
    ) {
        super(name, filter, null, false, properties);
        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setReadTimeoutMilli(10_000);
        builder.setConnectionTimeoutMilli(5_000);
        builder.setSslEnabled(false);
        builder.setAckResponseMode(false);
        builder.setBufferChunkInitialSize((1 << 20));
        builder.setBufferChunkRetentionSize(4 * (1 << 20));
        builder.setWaitUntilBufferFlushed(10);
        builder.setWaitUntilFlusherTerminated(10);
        builder.setFlushAttemptIntervalMillis(1000);
        builder.setMaxBufferSize(8L * (1 << 20));
        builder.setSenderMaxRetryCount(3);
        builder.setSenderBaseRetryIntervalMillis(1000);
        builder.setSenderMaxRetryIntervalMillis(5000);
        this.fluency = builder.build(host, port);
    }

    @PluginFactory
    public static FluentdAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("host") String host,
            @PluginAttribute("port") int port,
            @PluginElement("Filter") final Filter filter
    ) {
        return new FluentdAppender(name, host, port, filter, null);
    }

    @Override
    public void append(LogEvent event) {
        if (event.getLoggerName().startsWith(Fluency.class.getPackageName())) {
            //prevents endless logging loop in case of logs from Fluency
            return;
        }
        Filter filter = getFilter();
        if (filter != null) {
            switch (filter.filter(event)) {
                case ACCEPT, NEUTRAL -> { }
                default -> {
                    return;
                }
            }
        }
        try {
            var data = new HashMap<String, Object>();
            data.put(Fields.MESSAGE, event.getMessage().getFormattedMessage());
            data.put(Fields.LEVEL, event.getLevel().getStandardLevel());
            data.put(Fields.LOGGER, event.getLoggerName());
            data.put(Fields.THREAD, event.getThreadName());
            data.put(Fields.LOG_ID, UUID.randomUUID().toString());
            data.put(Fields.LOG_CONTEXT, event.getContextData().toMap());
            if (event.getThrown() != null) {
                data.put(Fields.STACKTRACE, extractStacktrace(event.getThrown()));
            }

            fluency.emit("log", EventTime.fromEpochMilli(event.getTimeMillis()), data);
        } catch (IOException e) {
            System.err.println("Couldn't send '" + event.getMessage().getFormattedMessage() + "' to fluentd: " + e);
        }
    }

    private static String extractStacktrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        t.printStackTrace(writer);
        return sw.toString();
    }

    public static class Fields {
        public static final String MESSAGE = "message";
        public static final String LOG_CONTEXT = "log_context";
        public static final String THREAD = "thread";
        public static final String LEVEL = "level";
        public static final String TIME = "time";
        public static final String LOGGER = "logger";
        public static final String LOG_ID = "log_id";
        public static final String STACKTRACE = "stacktrace";

    }
}
