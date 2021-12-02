package ru.yandex.cloud.ml.platform.lzy.server.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import ru.yandex.cloud.ml.platform.lzy.server.LzyServer;
import ru.yandex.cloud.ml.platform.lzy.server.configs.AgentsConfig;
import ru.yandex.cloud.ml.platform.lzy.server.configs.DbConfig;
import ru.yandex.cloud.ml.platform.lzy.server.hibernate.models.*;

import java.util.Set;

@Singleton
@Requires(property = "database.url")
@Requires(property = "database.username")
@Requires(property = "database.password")
public class Storage implements DbStorage{
    private static final Logger LOG = LogManager.getLogger(Storage.class);
    private final SessionFactory sessionFactory;

    @Inject
    public Storage(DbConfig config, AgentsConfig agents){

        Flyway flyway = Flyway.configure()
            .dataSource(config.getUrl(), config.getUsername(), config.getPassword())
            .locations("classpath:db/migrations")
            .load();
        flyway.migrate();
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", config.getUrl());
        cfg.setProperty("hibernate.connection.username", config.getUsername());
        cfg.setProperty("hibernate.connection.password", config.getPassword());
        cfg.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        cfg.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        cfg.addAnnotatedClass(UserModel.class);
        cfg.addAnnotatedClass(TaskModel.class);
        cfg.addAnnotatedClass(PublicKeyModel.class);
        cfg.addAnnotatedClass(UserRoleModel.class);
        cfg.addAnnotatedClass(PermissionModel.class);
        cfg.addAnnotatedClass(BackofficeSessionModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
        if (agents.getNames() == null || agents.getPublicKeys() == null || agents.getRoles() == null)
            return;
        int len = agents.getNames().size();
        if (len != agents.getPublicKeys().size() || len != agents.getRoles().size()){
            LOG.error("Length of agents.names, agents.public-keys and agents.roles must be equal");
            return;
        }
        for (int i = 0; i < agents.getNames().size(); i++) {

            String name = agents.getNames().get(i), key = agents.getPublicKeys().get(i), role = agents.getRoles().get(i);
            key = key.replaceAll("(\\\\n)", "\n");
            try(Session session = this.sessionFactory.openSession()){
                Transaction tx = session.beginTransaction();
                try {
                    UserModel user = session.find(UserModel.class, name);
                    if (user == null){
                        user = new UserModel(name);
                        session.save(user);
                    }
                    UserRoleModel roleModel = session.find(UserRoleModel.class, role);
                    Set<UserModel> users = roleModel.getUsers();
                    users.add(user);
                    roleModel.setUsers(users);
                    session.save(roleModel);
                    PublicKeyModel keyModel = session.find(PublicKeyModel.class, new PublicKeyModel.PublicKeyPk("main", name));
                    if (keyModel == null){
                        keyModel = new PublicKeyModel("main", key, name);
                    }
                    keyModel.setValue(key);
                    session.save(keyModel);
                    tx.commit();
                }
                catch (Exception e){
                    tx.rollback();
                    LOG.error(e);
                }
            }
        }
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
