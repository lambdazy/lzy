package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "backoffice_sessions")
public class BackofficeSessionModel {
    @ManyToOne
    @JoinColumn(name = "uid")
    UserModel owner;
    @Id
    @Column(name = "id", columnDefinition = "UUID")
    private UUID id;

    public BackofficeSessionModel(UUID id, UserModel owner) {
        this.id = id;
        this.owner = owner;
    }

    public BackofficeSessionModel() {
    }

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

    ;
}
