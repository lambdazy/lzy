package ai.lzy.test.context;

import ai.lzy.allocator.alloc.dao.SessionDao;
import io.micronaut.context.ApplicationContext;

public interface AllocatorBeans {
    ApplicationContext allocatorContext();

    default SessionDao sessionDao() {
        return allocatorContext().getBean(SessionDao.class);
    }
}
