package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "servants")
public class ServantModel {

    @Id
    @Column(name = "servant_id")
    private String servantId;

    @Column(name = "token")
    private String token;

    public ServantModel(String servantId, String token) {
        this.servantId = servantId;
        this.token = token;
    }

    public ServantModel() {}

    public String servantId() {
        return servantId;
    }

    public void setServantId(String servantId) {
        this.servantId = servantId;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String token() {
        return token;
    }
}
