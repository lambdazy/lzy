package ai.lzy.test.context;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.service.gc.GarbageCollector;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;

public interface LzyServiceBeans {
    ApplicationContext lzyServiceContext();

    default GarbageCollector garbageCollector() {
        return lzyServiceContext().getBean(GarbageCollector.class);
    }

    default WorkflowDao wfDao() {
        return lzyServiceContext().getBean(WorkflowDao.class);
    }

    default OperationDao operationDao() {
        return lzyServiceContext().getBean(OperationDao.class, Qualifiers.byName("LzyServiceOperationDao"));
    }
}
