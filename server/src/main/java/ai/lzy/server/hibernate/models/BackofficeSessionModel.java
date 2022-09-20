package ai.lzy.server.hibernate.models;

import javax.persistence.*;

@Entity
@Table(name = "backoffice_sessions")
public class BackofficeSessionModel {
    @ManyToOne
    @JoinColumn(name = "uid")
    UserModel owner;
    @Id
    @Column(name = "id")
    private String id;

    public BackofficeSessionModel(String id, UserModel owner) {
        this.id = id;
        this.owner = owner;
    }

    public BackofficeSessionModel() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
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
