package ai.lzy.scheduler.allocator;

import ai.lzy.scheduler.db.ServantDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ServantsAllocatorBase implements ServantsAllocator {
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);

    protected final ServantDao dao;
    protected final ServantMetaStorage metaStorage;

    protected ServantsAllocatorBase(ServantDao dao, ServantMetaStorage metaStorage) {
        this.dao = dao;
        this.metaStorage = metaStorage;
    }
}
