package ai.lzy.scheduler.db;

import ai.lzy.longrunning.dao.OperationDaoImpl;
import jakarta.inject.Singleton;

@Singleton
public class JobsOperationDao extends OperationDaoImpl {
    public JobsOperationDao(SchedulerDataSource storage) {
        super(storage);
    }
}
