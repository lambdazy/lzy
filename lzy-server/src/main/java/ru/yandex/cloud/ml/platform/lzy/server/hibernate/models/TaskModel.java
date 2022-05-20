package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;

@Entity
@Table(name = "tasks")
public class TaskModel {

    @Id
    @Column(name = "tid")
    private String tid;

    @ManyToOne()
    @JoinColumn(name = "owner_id", nullable = false)
    private UserModel owner;

    @ManyToOne()
    @JoinColumn(name = "servant_id", nullable = false)
    private ServantModel servant;

    public TaskModel(String tid, UserModel owner, ServantModel servant) {
        this.tid = tid;
        this.owner = owner;
        this.servant = servant;
    }

    public TaskModel() {
    }

    public String getTid() {
        return tid;
    }

    public UserModel getOwner() {
        return owner;
    }

    public ServantModel servant() {
        return servant;
    }
}
