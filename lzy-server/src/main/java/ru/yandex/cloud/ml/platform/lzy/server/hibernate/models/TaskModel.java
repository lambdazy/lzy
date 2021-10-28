package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import ru.yandex.cloud.ml.platform.lzy.server.task.Task;

import javax.persistence.*;
import java.util.UUID;

@Entity
@Table(name = "tasks")
public class TaskModel {

    @Id
    @Column(name = "tid", columnDefinition = "UUID")
    private UUID tid;

    @Column(name = "token", nullable = false)
    private String token;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name="owner_id", nullable=false)
    private UserModel owner;

    public UUID getTid() {
        return tid;
    }

    public String getToken() {
        return token;
    }

    public UserModel getOwner() {
        return owner;
    }

    public TaskModel(UUID tid, String token, UserModel owner) {
        this.tid = tid;
        this.token = token;
        this.owner = owner;
    }

    public TaskModel() {}
}
