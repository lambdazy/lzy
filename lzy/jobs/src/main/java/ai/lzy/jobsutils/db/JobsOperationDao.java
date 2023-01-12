package ai.lzy.jobsutils.db;

import ai.lzy.longrunning.dao.OperationDaoImpl;
import jakarta.inject.Singleton;

@Singleton
public class JobsOperationDao extends OperationDaoImpl {
    public JobsOperationDao(JobsDataSource storage) {
        super(storage);
    }
}
