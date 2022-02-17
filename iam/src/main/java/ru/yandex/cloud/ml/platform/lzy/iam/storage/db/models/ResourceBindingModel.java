package ru.yandex.cloud.ml.platform.lzy.iam.storage.db.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "user_resource_roles")
public class ResourceBindingModel {


    public ResourceBindingModel() {
    }

    public ResourceBindingModel(String userId, String resourceId, String resourceType, String role) {
        this.userId = userId;
        this.resourceId = resourceId;
        this.resourceType = resourceType;
        this.role = role;
    }

    @Column(name = "user_id")
    private String userId;

    @Column(name = "resource_id")
    private String resourceId;

    @Column(name = "resource_type")
    private String resourceType;

    @Column(name = "role")
    private String role;

    public String userId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String resourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String resourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public String role() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
}
