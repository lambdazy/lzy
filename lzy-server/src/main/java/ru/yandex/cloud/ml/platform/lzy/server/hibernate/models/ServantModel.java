package ru.yandex.cloud.ml.platform.lzy.server.hibernate.models;

import javax.persistence.*;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(name = "servants")
public class ServantModel {

    @Id
    @Column(name = "servant_id")
    private String servantId;

    @Column(name = "token")
    private String token;

    @OneToMany(mappedBy = "servant")
    private Set<ServantModel> servants;

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

    public Set<ServantModel> servants() {
        return servants;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String token() {
        return token;
    }
}
