package ai.lzy.servant.logs;

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

@Plugin(name = "ServantAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class ServantAppender extends AbstractAppender {
    private static final Lock lock = new ReentrantLock();
    private static ServantAppender instance;
    private final ConcurrentLinkedQueue<LogEvent> queue;
    private final String servantId;
    private BigInteger eventId = BigInteger.valueOf(0);

    protected ServantAppender(String name, Filter filter, String servantId, Layout<?> layout) {
        super(name, filter, layout, true);
        this.servantId = servantId;
        this.queue = new ConcurrentLinkedQueue<>();
        Thread thread = new Thread(this::queueThread);
        thread.start();
    }

    @PluginFactory
    public static ServantAppender createAppender(
        @PluginAttribute("name") String name,
        @PluginElement("Layout") Layout<? extends Serializable> layout,
        @PluginElement("Filter") final Filter filter,
        @PluginAttribute("servantId") String servantId
    )
    {
        lock.lock();
        try {
            if (instance == null) {
                instance = new ServantAppender(name, filter, servantId, layout);
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
            "INSERT INTO lzy.servant_logs "
                + "(EVENT_ID, EVENT_DATE, LEVEL, LOGGER, MESSAGE, SERVANT_ID) "
                + "VALUES (?, ?, ?, ?, ?, ?)"
        );
        stmt.setString(1, eventId);
        stmt.setString(2, eventDate);
        stmt.setString(3, level);
        stmt.setString(4, logger);
        stmt.setString(5, message);
        stmt.setString(6, servantId);
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
