package ai.lzy.whiteboard.hibernate;

import org.hibernate.SessionFactory;

public interface DbStorage {
    SessionFactory getSessionFactory();
}
