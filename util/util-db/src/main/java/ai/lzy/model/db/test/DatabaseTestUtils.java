package ai.lzy.model.db.test;

import ai.lzy.model.db.DatabaseConfiguration;
import ai.lzy.model.db.Storage;

import java.sql.SQLException;
import java.util.HashMap;

public enum DatabaseTestUtils {
    ;

    @SuppressWarnings("checkstyle:Indentation")
    public static HashMap<String, Object> preparePostgresConfig(String app, Object ci) {
        /* io.zonky.test.db.postgres.embedded.ConnectionInfo ci */
        try {
            var user = getFieldValue(ci, "user", String.class);
            assert "postgres".equals(user);

            var port = getFieldValue(ci, "port", Integer.class);
            var dbName = getFieldValue(ci, "dbName", String.class);

            return new HashMap<>() {{
                put(app + ".database.enabled", "true");
                put(app + ".database.url", "jdbc:postgresql://localhost:%d/%s".formatted(port, dbName));
                put(app + ".database.username", "postgres");
                put(app + ".database.password", "");
            }};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static DatabaseConfiguration preparePostgresConfig(Object ci) {
        /* io.zonky.test.db.postgres.embedded.ConnectionInfo ci */
        try {
            var user = getFieldValue(ci, "user", String.class);
            assert "postgres".equals(user);

            var port = getFieldValue(ci, "port", Integer.class);
            var dbName = getFieldValue(ci, "dbName", String.class);

            var result = new DatabaseConfiguration();
            result.setEnabled(true);
            result.setUrl("jdbc:postgresql://localhost:%d/%s".formatted(port, dbName));
            result.setUsername("postgres");
            result.setPassword("");
            result.setMinPoolSize(1);
            result.setMaxPoolSize(10);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("checkstyle:Indentation")
    public static HashMap<String, Object> prepareLocalhostConfig(String app) {
        return new HashMap<>() {{
            put(app + ".database.enabled", "true");
            put(app + ".database.url", "jdbc:postgresql://localhost:5432/lzy_%s_db"
                .formatted(app.replaceAll("-", "_")));
            put(app + ".database.username", "lzy_user");
            put(app + ".database.password", "q");
        }};
    }

    public static void cleanup(Storage storage) {
        try (var conn = storage.connect()) {
            if (conn.getMetaData().getDatabaseProductName().startsWith("PostgreSQL")) {
                conn.prepareStatement("DROP SCHEMA public CASCADE").execute();
                conn.prepareStatement("CREATE SCHEMA public").execute();
                conn.prepareStatement("GRANT ALL ON SCHEMA public TO postgres").execute();
                conn.prepareStatement("GRANT ALL ON SCHEMA public TO public").execute();
            } else {
                var st = conn.prepareStatement("DROP ALL OBJECTS DELETE FILES");
                st.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    private static <T> T getFieldValue(Object obj, String fieldName, Class<T> type) throws Exception {
        var field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return type.cast(field.get(obj));
    }
}
