package ai.lzy.storage;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.storage.data.StorageDataSource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {
    public static final String DAO_NAME = "StorageOperationDao";

    @Singleton
    @Requires(beans = StorageDataSource.class)
    @Named(DAO_NAME)
    public OperationDao operationDao(StorageDataSource storage) {
        return new OperationDaoImpl(storage);
    }
}
