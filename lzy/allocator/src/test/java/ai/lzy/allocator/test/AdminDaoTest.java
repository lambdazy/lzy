package ai.lzy.allocator.test;

import ai.lzy.allocator.admin.dao.AdminDao;
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
import java.util.List;
import java.util.stream.Stream;

import static ai.lzy.allocator.model.ActiveImages.DindImages;
import static ai.lzy.allocator.model.ActiveImages.Image;
import static ai.lzy.allocator.model.ActiveImages.PoolConfig;

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
        Assert.assertTrue(conf.configs().isEmpty());
        Assert.assertNull(conf.sync().image());
    }

    @Test
    public void images() throws SQLException {
        adminDao.setImages(List.of(
            PoolConfig.of(List.of(), DindImages.of("d1", List.of()), "CPU", "s"),
            PoolConfig.of(List.of(Image.of("w1"), Image.of("w2")), DindImages.of("d2", List.of("di1")), "CPU", "m")
        ));

        var conf = adminDao.getImages();
        List<String> workers = conf.configs().stream().flatMap(pc -> pc.images().stream().map(Image::image)).sorted(String::compareTo).toList();
        var dind = conf.configs().stream().flatMap(pc -> pc.dindImages().additionalImages().stream()).sorted(String::compareTo).toList();
        var dindSync = conf.configs().stream().map(pc -> pc.dindImages().dindImage()).sorted(String::compareTo).toList();
        Assert.assertEquals(List.of("w1", "w2"), workers);
        Assert.assertEquals(List.of("di1"), dind);
        Assert.assertEquals(List.of("d1", "d2"), dindSync);
        Assert.assertNull(conf.sync().image());

        adminDao.setImages(List.of(
            PoolConfig.of(List.of(Image.of("w3")), DindImages.of("d3", List.of()), "GPU", "m"),
            PoolConfig.of(List.of(), null, "CPU", "name4")
        ));

        conf = adminDao.getImages();
        workers = conf.configs().stream().flatMap(pc -> pc.images().stream().map(Image::image)).sorted(String::compareTo).toList();
        dind = conf.configs().stream()
            .flatMap(pc -> {
                if (pc.dindImages() == null) {
                    return Stream.empty();
                } else {
                    return pc.dindImages().additionalImages().stream();
                }
            })
            .sorted(String::compareTo).toList();
        dindSync = conf.configs().stream()
            .flatMap(pc -> {
                if (pc.dindImages() == null) {
                    return Stream.empty();
                } else {
                    return Stream.of(pc.dindImages().dindImage());
                }
            })
            .sorted(String::compareTo).toList();
        Assert.assertEquals(List.of("w3"), workers);
        Assert.assertTrue(dind.isEmpty());
        Assert.assertEquals(List.of("d3"), dindSync);
        Assert.assertNull(conf.sync().image());
    }

    @Test
    public void sync() throws SQLException {
        adminDao.setSyncImage(Image.of("sync1"));

        var conf = adminDao.getImages();
        Assert.assertTrue(conf.configs().isEmpty());
        Assert.assertEquals("sync1", conf.sync().image());

        adminDao.setSyncImage(Image.of("sync2"));

        conf = adminDao.getImages();
        Assert.assertTrue(conf.configs().isEmpty());
        Assert.assertEquals("sync2", conf.sync().image());
    }
}
