package ai.lzy.worker.logs;

import ru.yandex.clickhouse.ClickHouseDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionFactory {

    private static ClickHouseDataSource dataSource;

    public static Connection getDatabaseConnection() throws SQLException {
        if (dataSource == null) {
            String clickhouseHost = System.getenv("LOGS_HOST");
            String clickhouseUser = System.getenv("LOGS_USER");
            String clickhousePass = System.getenv("LOGS_PASSWORD");
            String url = String.format("jdbc:clickhouse://%s:8123?user=%s&password=%s",
                clickhouseHost, clickhouseUser, clickhousePass);
            dataSource = new ClickHouseDataSource(url);
        }
        return dataSource.getConnection();
    }
}
