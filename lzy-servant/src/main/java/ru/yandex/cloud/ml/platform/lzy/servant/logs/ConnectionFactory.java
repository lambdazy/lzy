package ru.yandex.cloud.ml.platform.lzy.servant.logs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactory {
    private interface Singleton {
        ConnectionFactory INSTANCE = new ConnectionFactory();
    }

    private final DataSource dataSource;

    private ConnectionFactory() {
        String clickhouseHost = System.getenv("LOGS_HOST");
        String clickhouseUser = System.getenv("LOGS_USER");
        String clickhousePass = System.getenv("LOGS_PASSWORD");
        String url = String.format("jdbc:clickhouse://%s:8123/lzy?user=%s&password=%s",
                clickhouseHost, clickhouseUser, clickhousePass);

        this.dataSource = new ClickHouseDataSource(url);
    }

    public static Connection getDatabaseConnection() throws SQLException {
        return Singleton.INSTANCE.dataSource.getConnection();
    }
}
