package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.UserModel;
import ru.yandex.cloud.ml.platform.lzy.server.task.Task;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Storage implements Authenticator {
    private final SessionFactory sessionFactory;
    public final UserDAO users;
    private final Map<String, String> taskTokens = new HashMap<>();
    private final Map<String, String> owners = new HashMap<>();

    public Storage(){
        Configuration cfg = new Configuration().configure();
        cfg.setProperty("hibernate.connection.url", System.getenv("DATABASE_URL"));
        cfg.setProperty("hibernate.connection.username", System.getenv("DATABASE_USERNAME"));
        cfg.setProperty("hibernate.connection.password", System.getenv("DATABASE_PASSWORD"));
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.addAnnotatedClass(UserModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
        this.users = new UserDAO(this);
    }

    public SessionFactory getSessionFactory(){
        return sessionFactory;
    }

    @Override
    public boolean checkUser(String userId, String token, String tokenSign) {
        return users.isUserTokenSigned(userId, token, tokenSign);
    }

    @Override
    public boolean checkTask(String tid, String token) {
        return true;
    }

    @Override
    public boolean canPublish(String userId) {
        return true;
    }

    @Override
    public boolean canAccess(String zygoteName, String user) {
        return true;
    }

    @Override
    public boolean canAccess(Task task, String user) {
        return true;
    }

    @Override
    public String userForTask(Task task) {
        return owners.get(task.tid().toString());
    }

    @Override
    public void registerOperation(String zygoteName, String userId, Lzy.PublishRequest.VisibilityScope scope) {
    }

    @Override
    public String registerTask(String uid, Task task) {
        owners.put(task.tid().toString(), uid);
        final String token = UUID.randomUUID().toString();
        taskTokens.put(task.tid().toString(), token);
        return token;
    }
}
