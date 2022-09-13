package ai.lzy.site.oauth.models;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class GitHubGetUserResponse {

    private String login;
    private String id;

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
