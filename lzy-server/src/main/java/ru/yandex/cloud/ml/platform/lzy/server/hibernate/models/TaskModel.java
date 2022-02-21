package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "tasks")
public class TaskModel {

    @Id
    @Column(name = "tid", columnDefinition = "UUID")
    private UUID tid;

    @Column(name = "token", nullable = false)
    private String token;

    @ManyToOne()
    @JoinColumn(name = "owner_id", nullable = false)
    private UserModel owner;

    public TaskModel(UUID tid, String token, UserModel owner) {
        this.tid = tid;
        this.token = token;
        this.owner = owner;
    }

    public TaskModel() {
    }

    public UUID getTid() {
        return tid;
    }

    public String getToken() {
        return token;
    }

    public UserModel getOwner() {
        return owner;
    }
}
