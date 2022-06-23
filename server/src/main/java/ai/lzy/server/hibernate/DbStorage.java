package ai.lzy.server.hibernate;

import org.hibernate.SessionFactory;

public interface DbStorage {
    SessionFactory getSessionFactory();
}
