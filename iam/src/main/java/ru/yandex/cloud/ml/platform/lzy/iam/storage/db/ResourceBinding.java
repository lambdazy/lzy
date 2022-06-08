package ru.yandex.cloud.ml.platform.lzy.iam.storage.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResourceBinding {

    private final String userId;
    private final String resourceId;
    private final String resourceType;
    private final String role;

    public ResourceBinding(String userId, String resourceId, String resourceType, String role) {
        this.userId = userId;
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.role = role;
    }

    public static ResourceBinding fromResultSet(ResultSet rs) throws SQLException {
        return new ResourceBinding(
                rs.getString("user_id"),
                rs.getString("resource_id"),
                rs.getString("resource_type"),
                rs.getString("role")
        );
    }

    public String userId() {
        return userId;
    }

    public String resourceId() {
        return resourceId;
    }

    public String resourceType() {
        return resourceType;
    }

    public String role() {
        return role;
    }
}
