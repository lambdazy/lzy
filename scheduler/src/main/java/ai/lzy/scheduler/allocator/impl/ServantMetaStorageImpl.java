package ai.lzy.scheduler.allocator.impl;

import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.db.ServantMetaStorageDao;
import ai.lzy.scheduler.db.ServantMetaStorageDao.MetaStorageEntry;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ServantMetaStorageImpl implements ServantMetaStorage {
    private final ServantMetaStorageDao dao;

    @Inject
    public ServantMetaStorageImpl(ServantMetaStorageDao dao) {
        this.dao = dao;
    }

    @Override
    public void clear(String workflowId, String servantId) {
        dao.remove(workflowId, servantId);
    }

    @Override
    public void saveMeta(String workflowId, String servantId, String meta) {
        final MetaStorageEntry entry = dao.get(workflowId, servantId);
        if (entry == null) {
            dao.save(new MetaStorageEntry(workflowId, servantId, meta, null));
            return;
        }
        dao.save(new MetaStorageEntry(workflowId, servantId, meta, entry.token()));
    }

    @Override
    public String getMeta(String workflowId, String servantId) {
        final MetaStorageEntry entry = dao.get(workflowId, servantId);
        if (entry == null) {
            return null;
        }
        return entry.meta();
    }
}
