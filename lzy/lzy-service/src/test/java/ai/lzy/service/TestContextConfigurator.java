package ai.lzy.service;

import ai.lzy.allocator.test.BaseTestWithAllocator;
import ai.lzy.channelmanager.test.BaseTestWithChannelManager;
import ai.lzy.graph.test.BaseTestWithGraphExecutor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.service.test.LzyServiceTestContext;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.storage.test.BaseTestWithStorage;
import io.zonky.test.db.postgres.embedded.ConnectionInfo;

import java.sql.SQLException;
import java.util.HashMap;

import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

public abstract class TestContextConfigurator {
    private TestContextConfigurator() {}

    public static void setUp(
        BaseTestWithIam iamTestContext, ConnectionInfo iamDbConn,
        BaseTestWithStorage s3TestContext, ConnectionInfo s3DbConn,
        BaseTestWithChannelManager channelManagerTestContext, ConnectionInfo channelManagerDbConn,
        BaseTestWithGraphExecutor graphExecutorTestContext, ConnectionInfo graphExecutorDbConn,
        BaseTestWithAllocator allocatorTestContext, ConnectionInfo allocatorDbConn,
        LzyServiceTestContext lzyServiceTestContext, ConnectionInfo lzyServiceDbConn
    ) throws Exception
    {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);

        iamTestContext.setUp(preparePostgresConfig("iam", iamDbConn));

        s3TestContext.setUp(new HashMap<>() {
            {
                put("storage.iam.address", iamTestContext.getAddress());
                putAll(preparePostgresConfig("storage", s3DbConn));
            }
        });

        channelManagerTestContext.setUp(new HashMap<>() {
            {
                put("channel-manager.iam.address", iamTestContext.getAddress());
                putAll(preparePostgresConfig("channel-manager", channelManagerDbConn));
            }
        });

        graphExecutorTestContext.setUp(new HashMap<>() {
            {
                put("graph-executor.iam.address", iamTestContext.getAddress());
                putAll(preparePostgresConfig("graph-executor", graphExecutorDbConn));
            }
        });

        allocatorTestContext.setUp(new HashMap<>() {
            {
                put("allocator.thread-allocator.enabled", true);
                put("allocator.thread-allocator.vm-class-name", "ai.lzy.portal.App");
                put("allocator.iam.address", iamTestContext.getAddress());
                putAll(preparePostgresConfig("allocator", allocatorDbConn));
            }
        });

        lzyServiceTestContext.setUp(new HashMap<>() {
            {
                put("lzy-service.iam.address", iamTestContext.getAddress());
                put("lzy-service.storage.address", s3TestContext.getAddress());
                put("lzy-service.channel-manager-address", channelManagerTestContext.getAddress());
                put("lzy-service.graph-executor-address", graphExecutorTestContext.getAddress());
                put("lzy-service.allocator-address", allocatorTestContext.getAddress());
                putAll(preparePostgresConfig("lzy-service", lzyServiceDbConn));
            }
        });
    }

    public static void tearDown(
        BaseTestWithIam iamTestContext,
        BaseTestWithStorage s3TestContext,
        BaseTestWithChannelManager channelManagerTestContext,
        BaseTestWithGraphExecutor graphExecutorTestContext,
        BaseTestWithAllocator allocatorTestContext,
        LzyServiceTestContext lzyServiceTestContext
    ) throws SQLException, InterruptedException
    {
        lzyServiceTestContext.after();
        graphExecutorTestContext.after();
        allocatorTestContext.after();
        channelManagerTestContext.after();
        s3TestContext.after();
        iamTestContext.after();
    }
}
