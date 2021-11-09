package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.UUID;

@Entity
@Table(name = "backoffice_sessions")
public class BackofficeSessionModel {
    @Id
    @Column(name = "id", columnDefinition = "UUID")
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "uid")
    UserModel owner;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UserModel getOwner() {
        return owner;
    }

    public void setOwner(UserModel owner) {
        this.owner = owner;
    }

    public BackofficeSessionModel(UUID id, UserModel owner) {
        this.id = id;
        this.owner = owner;
    }

    public BackofficeSessionModel(){};
}
