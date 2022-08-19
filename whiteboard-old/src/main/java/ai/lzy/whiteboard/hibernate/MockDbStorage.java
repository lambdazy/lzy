package ai.lzy.whiteboard.hibernate;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import ai.lzy.whiteboard.hibernate.models.EntryDependenciesModel;
import ai.lzy.whiteboard.hibernate.models.ExecutionModel;
import ai.lzy.whiteboard.hibernate.models.InputArgModel;
import ai.lzy.whiteboard.hibernate.models.OutputArgModel;
import ai.lzy.whiteboard.hibernate.models.SnapshotEntryModel;
import ai.lzy.whiteboard.hibernate.models.SnapshotModel;
import ai.lzy.whiteboard.hibernate.models.WhiteboardFieldModel;
import ai.lzy.whiteboard.hibernate.models.WhiteboardModel;
import ai.lzy.whiteboard.hibernate.models.WhiteboardTagModel;

@Singleton
@Requires(missingProperty = "database.url")
@Requires(missingProperty = "database.username")
@Requires(missingProperty = "database.password")
public class MockDbStorage implements DbStorage {

    private final SessionFactory sessionFactory;

    public MockDbStorage() {
        Configuration cfg = new Configuration();
        cfg.setProperty("hibernate.connection.url", "jdbc:h2:mem:testdb_wb;DB_CLOSE_DELAY=-1");
        cfg.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        cfg.setProperty("hibernate.dialect",
            "org.hibernate.dialect.H2Dialect");
        cfg.setProperty("hibernate.hbm2ddl.auto", "create");
        cfg.addAnnotatedClass(EntryDependenciesModel.class);
        cfg.addAnnotatedClass(WhiteboardFieldModel.class);
        cfg.addAnnotatedClass(SnapshotEntryModel.class);
        cfg.addAnnotatedClass(WhiteboardModel.class);
        cfg.addAnnotatedClass(SnapshotModel.class);
        cfg.addAnnotatedClass(WhiteboardTagModel.class);
        cfg.addAnnotatedClass(ExecutionModel.class);
        cfg.addAnnotatedClass(OutputArgModel.class);
        cfg.addAnnotatedClass(InputArgModel.class);
        this.sessionFactory = cfg.buildSessionFactory();
    }

    @Override
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }
}
