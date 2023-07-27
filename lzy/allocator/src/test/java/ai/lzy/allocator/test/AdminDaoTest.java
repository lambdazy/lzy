package ai.lzy.allocator.test;

import ai.lzy.allocator.admin.dao.AdminDao;
import ai.lzy.allocator.model.ActiveImages.JupyterLabImage;
import ai.lzy.allocator.model.ActiveImages.SyncImage;
import ai.lzy.allocator.model.ActiveImages.WorkerImage;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.model.db.test.DatabaseTestUtils;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;

public class AdminDaoTest {
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private AdminDao adminDao;

    @Before
    public void setUp() {
        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("allocator", db.getConnectionInfo()));
        adminDao = context.getBean(AdminDao.class);
    }

    @After
    public void tearDown() {
        context.getBean(AllocatorDataSource.class).setOnClose(DatabaseTestUtils::cleanup);
        context.stop();
    }

    @Test
    public void emptyOnStart() throws SQLException {
        var conf = adminDao.getImages();
        Assert.assertTrue(conf.workers().isEmpty());
        Assert.assertNull(conf.sync().image());
        Assert.assertTrue(conf.jupyterLabs().isEmpty());
    }

    @Test
    public void workers() throws SQLException {
        adminDao.setWorkerImages(List.of(WorkerImage.of("w1"), WorkerImage.of("w2")));

        var conf = adminDao.getImages();
        var workers = conf.workers().stream().map(WorkerImage::image).sorted(String::compareTo).toList();
        Assert.assertEquals(List.of("w1", "w2"), workers);
        Assert.assertNull(conf.sync().image());
        Assert.assertTrue(conf.jupyterLabs().isEmpty());

        adminDao.setWorkerImages(List.of(WorkerImage.of("w3"), WorkerImage.of("w4")));

        conf = adminDao.getImages();
        workers = conf.workers().stream().map(WorkerImage::image).sorted(String::compareTo).toList();
        Assert.assertEquals(List.of("w3", "w4"), workers);
        Assert.assertNull(conf.sync().image());
        Assert.assertTrue(conf.jupyterLabs().isEmpty());
    }

    @Test
    public void sync() throws SQLException {
        adminDao.setSyncImage(SyncImage.of("sync1"));

        var conf = adminDao.getImages();
        Assert.assertTrue(conf.workers().isEmpty());
        Assert.assertEquals("sync1", conf.sync().image());
        Assert.assertTrue(conf.jupyterLabs().isEmpty());

        adminDao.setSyncImage(SyncImage.of("sync2"));

        conf = adminDao.getImages();
        Assert.assertTrue(conf.workers().isEmpty());
        Assert.assertEquals("sync2", conf.sync().image());
        Assert.assertTrue(conf.jupyterLabs().isEmpty());
    }

    @Test
    public void jupyterLab() throws SQLException {
        adminDao.setJupyterLabImages(List.of(
            JupyterLabImage.of("jl1", new String[] {}),
            JupyterLabImage.of("jl2", new String[] {"img1", "img2"})));

        var conf = adminDao.getImages();
        Assert.assertTrue(conf.workers().isEmpty());
        Assert.assertNull(conf.sync().image());
        var jls = conf.jupyterLabs().stream().sorted(Comparator.comparing(JupyterLabImage::mainImage)).toList();
        Assert.assertEquals(JupyterLabImage.of("jl1", new String[] {}), jls.get(0));
        Assert.assertEquals(JupyterLabImage.of("jl2", new String[] {"img1", "img2"}), jls.get(1));

        adminDao.setJupyterLabImages(List.of(
            JupyterLabImage.of("jl2", new String[] {}),
            JupyterLabImage.of("jl3", new String[] {"img3", "img4"})));

        conf = adminDao.getImages();
        Assert.assertTrue(conf.workers().isEmpty());
        Assert.assertNull(conf.sync().image());
        jls = conf.jupyterLabs().stream().sorted(Comparator.comparing(JupyterLabImage::mainImage)).toList();
        Assert.assertEquals(JupyterLabImage.of("jl2", new String[] {}), jls.get(0));
        Assert.assertEquals(JupyterLabImage.of("jl3", new String[] {"img3", "img4"}), jls.get(1));
    }
}
