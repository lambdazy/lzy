package ai.lzy.model.db;

public class DatabaseConfiguration {
    private String url;
    private String username;
    private String password;
    private int minPoolSize;
    private int maxPoolSize;

    public String url() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String username() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String password() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int minPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public int maxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }
}
