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

    @ManyToOne()
    @JoinColumn(name = "owner_id", nullable = false)
    private UserModel owner;

    @ManyToOne()
    @JoinColumn(name = "servant_id", nullable = false)
    private ServantModel servant;

    public TaskModel(UUID tid, UserModel owner, ServantModel servant) {
        this.tid = tid;
        this.owner = owner;
        this.servant = servant;
    }

    public TaskModel() {
    }

    public UUID getTid() {
        return tid;
    }

    public UserModel getOwner() {
        return owner;
    }

    public ServantModel servant() {
        return servant;
    }
}
