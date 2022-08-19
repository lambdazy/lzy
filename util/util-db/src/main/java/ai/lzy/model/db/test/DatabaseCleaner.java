package ai.lzy.model.db.test;

import ai.lzy.model.db.Storage;

import java.sql.SQLException;

public enum DatabaseCleaner {
    ;

    public static void cleanup(Storage storage) {
        try (var conn = storage.connect(); var st = conn.prepareStatement("DROP ALL OBJECTS DELETE FILES")) {
            st.executeUpdate();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
