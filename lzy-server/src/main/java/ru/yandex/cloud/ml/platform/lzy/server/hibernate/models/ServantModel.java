package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(name = "servants")
public class ServantModel {

    @Id
    @Column(name = "servant_id")
    private UUID servantId;

    @Column(name = "token")
    private String token;

    public ServantModel(UUID servantId, String token) {
        this.servantId = servantId;
        this.token = token;
    }

    public ServantModel() {}

    public UUID servantId() {
        return servantId;
    }

    public void setServantId(UUID servantId) {
        this.servantId = servantId;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String token() {
        return token;
    }
}
