package ai.lzy.worker.logs;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Plugin(name = "WorkerAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class WorkerAppender extends AbstractAppender {
    private static final Lock lock = new ReentrantLock();
    private static WorkerAppender instance;
    private final ConcurrentLinkedQueue<LogEvent> queue;
    private final String workerId;
    private BigInteger eventId = BigInteger.valueOf(0);

    protected WorkerAppender(String name, Filter filter, String workerId, Layout<?> layout) {
        super(name, filter, layout, true);
        this.workerId = workerId;
        this.queue = new ConcurrentLinkedQueue<>();
        Thread thread = new Thread(this::queueThread);
        thread.start();
    }

    @PluginFactory
    public static WorkerAppender createAppender(
        @PluginAttribute("name") String name,
        @PluginElement("Layout") Layout<? extends Serializable> layout,
        @PluginElement("Filter") final Filter filter,
        @PluginAttribute("workerId") String workerId
    )
    {
        lock.lock();
        try {
            if (instance == null) {
                instance = new WorkerAppender(name, filter, workerId, layout);
            }
            return instance;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void append(LogEvent event) {
        queue.add(event);
    }

    private void log(String eventId, String eventDate, String level, String logger, String message)
        throws SQLException
    {
        Connection connection = ConnectionFactory.getDatabaseConnection();
        PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO lzy.worker_logs "
                + "(EVENT_ID, EVENT_DATE, LEVEL, LOGGER, MESSAGE, WORKER_ID) "
                + "VALUES (?, ?, ?, ?, ?, ?)"
        );
        stmt.setString(1, eventId);
        stmt.setString(2, eventDate);
        stmt.setString(3, level);
        stmt.setString(4, logger);
        stmt.setString(5, message);
        stmt.setString(6, workerId);
        stmt.executeUpdate();
        connection.close();
    }

    private void queueThread() {
        while (true) {
            LogEvent event;
            event = queue.poll();
            if (event == null) {
                continue;
            }
            try {
                log(eventId.toString(), new Date(event.getTimeMillis()).toString(), event.getLevel().toString(),
                    event.getLoggerFqcn(), event.getMessage().getFormattedMessage());
                eventId = eventId.add(BigInteger.valueOf(1));
            } catch (SQLException e) {
                error("Cannot log. Message: " + event.getMessage().getFormattedMessage());
                error("Sql error: " + e.getMessage());
            }
        }
    }
}
