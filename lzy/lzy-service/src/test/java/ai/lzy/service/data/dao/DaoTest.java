package ai.lzy.service.data.dao;

import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.v1.common.LMST;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.apache.commons.collections4.SetUtils;
import org.junit.*;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DaoTest {
    @Rule
    public PreparedDbRule lzyServiceDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext lzyStorageCtx;

    private WorkflowDao workflowDao;
    private ExecutionDao executionDao;

    @Before
    public void setUp() {
        var lzyDbConfig = DatabaseTestUtils.preparePostgresConfig("lzy-service", lzyServiceDb.getConnectionInfo());
        lzyStorageCtx = ApplicationContext.run(PropertySource.of(lzyDbConfig));

        workflowDao = lzyStorageCtx.getBean(WorkflowDaoImpl.class);
        executionDao = lzyStorageCtx.getBean(ExecutionDaoImpl.class);
    }

    @After
    public void tearDown() {
        lzyStorageCtx.stop();
    }

    @Test
    public void createExecutionWhenWorkflowAlreadyHasActive() throws SQLException {
        var firstExecutionId = "execution_1";
        var secondExecutionId = "execution_2";

        var userId = "user_1";
        var workflowName = "workflow_1";

        var storageType = "USER";
        var storageConfig = LMST.StorageConfig.newBuilder().setUri("uri_1").build();

        var initialActive = create(userId, workflowName, firstExecutionId, storageType, storageConfig);
        var previousActive = create(userId, workflowName, secondExecutionId, storageType, storageConfig);

        var sc1 = executionDao.getStorageConfig(firstExecutionId);
        var sc2 = executionDao.getStorageConfig(secondExecutionId);

        assertEquals(storageConfig.getUri(), sc1.getUri());
        assertEquals(storageConfig.getUri(), sc2.getUri());

        assertNull(initialActive);
        assertEquals(firstExecutionId, previousActive);
    }

    @Test
    public void putSlotSnapshotsGetSnapshots() throws SQLException {
        var firstExecutionId = "execution_1";
        var secondExecutionId = "execution_2";

        var userId = "user_1";
        var firstWorkflowName = "workflow_1";
        var secondWorkflowName = "workflow_2";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        var firstSlotsUri = Set.of("slot_snapshot_1", "slot_snapshot_3");
        var secondSlotsUri = Set.of("slot_snapshot_2");
        var allSlotsUri = SetUtils.union(firstSlotsUri, secondSlotsUri);

        create(userId, firstWorkflowName, firstExecutionId, storageType, s3Locator);
        create(userId, secondWorkflowName, secondExecutionId, storageType, s3Locator);

        executionDao.saveSlots(firstExecutionId, firstSlotsUri, null);
        executionDao.saveSlots(secondExecutionId, secondSlotsUri, null);

        Set<String> existingSlots = executionDao.retainExistingSlots(allSlotsUri);

        Set<String> nonExistForFirst = executionDao.retainNonExistingSlots(firstExecutionId, firstSlotsUri);
        Set<String> allSecondNonExistForFirst = executionDao.retainNonExistingSlots(firstExecutionId, secondSlotsUri);

        Set<String> nonExistForSecond = executionDao.retainNonExistingSlots(secondExecutionId, secondSlotsUri);
        Set<String> allFirstNonExistForSecond = executionDao.retainNonExistingSlots(secondExecutionId, firstSlotsUri);

        assertEquals(allSlotsUri, existingSlots);
        Assert.assertTrue(nonExistForFirst.isEmpty());
        assertEquals(secondSlotsUri, allSecondNonExistForFirst);
        Assert.assertTrue(nonExistForSecond.isEmpty());
        assertEquals(firstSlotsUri, allFirstNonExistForSecond);
    }

    @Test
    public void putSlotChannelsGetChannels() throws SQLException {
        var firstExecutionId = "execution_1";
        var secondExecutionId = "execution_2";

        var userId = "user_1";
        var firstWorkflowName = "workflow_1";
        var secondWorkflowName = "workflow_2";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        var firstSlotsUri = Set.of("slot_snapshot_1", "slot_snapshot_3");
        var secondSlotsUri = Set.of("slot_snapshot_2");

        var channels = Map.of(
            "slot_snapshot_1", "channel_1",
            "slot_snapshot_3", "channel_3"
        );

        create(userId, firstWorkflowName, firstExecutionId, storageType, s3Locator);
        create(userId, secondWorkflowName, secondExecutionId, storageType, s3Locator);

        executionDao.saveSlots(firstExecutionId, firstSlotsUri, null);
        executionDao.saveSlots(secondExecutionId, secondSlotsUri, null);

        executionDao.saveChannels(channels, null);

        Map<String, String> firstChannelsFromDao = executionDao.findChannels(firstSlotsUri);
        Map<String, String> secondChannelsFromDao = executionDao.findChannels(secondSlotsUri);

        assertEquals(channels, firstChannelsFromDao);
        Assert.assertTrue(secondChannelsFromDao.isEmpty());
    }

    @Test
    public void putSlotSnapshotForNonExistingExecution() throws SQLException {
        var firstExecutionId = "execution_1";
        var secondExecutionId = "execution_2";

        var userId = "user_1";
        var firstWorkflowName = "workflow_1";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        create(userId, firstWorkflowName, firstExecutionId, storageType, s3Locator);

        var slotsUri = Set.of("slot_snapshot_1", "slot_snapshot_2");

        Assert.assertThrows(SQLException.class, () -> executionDao.saveSlots(secondExecutionId, slotsUri, null));
        Set<String> existingSlots = executionDao.retainExistingSlots(slotsUri);

        Assert.assertTrue(existingSlots.isEmpty());
    }

    @Test
    public void putChannelForNonExistingSlot() throws SQLException {
        var executionId = "execution_1";

        var userId = "user_1";
        var workflowName = "workflow_1";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        var slotsUri = Set.of("slot_snapshot_1", "slot_snapshot_3");
        var unknownSlotUri = Set.of("slot_snapshot_2");
        var channels = Map.of("slot_snapshot_2", "channel_3");

        create(userId, workflowName, executionId, storageType, s3Locator);

        executionDao.saveSlots(executionId, slotsUri, null);

        Assert.assertThrows(SQLException.class, () -> executionDao.saveChannels(channels, null));
        Map<String, String> channelsFromDao = executionDao.findChannels(SetUtils.union(slotsUri, unknownSlotUri));
        Assert.assertTrue(channelsFromDao.isEmpty());
    }

    @Test
    public void putSlotSnapshotForAlreadyAssociatedSlot() throws SQLException {
        var executionId = "execution_1";

        var userId = "user_1";
        var firstWorkflowName = "workflow_1";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        var slotsUri = Set.of("slot_snapshot_1", "slot_snapshot_3");
        var oneMoreSlotUri = Set.of("slot_snapshot_2");
        var allSlotsUri = SetUtils.union(slotsUri, oneMoreSlotUri);

        create(userId, firstWorkflowName, executionId, storageType, s3Locator);

        executionDao.saveSlots(executionId, allSlotsUri, null);

        Assert.assertThrows(SQLException.class, () -> executionDao.saveSlots(executionId, oneMoreSlotUri, null));

        Set<String> existingSlots = executionDao.retainExistingSlots(allSlotsUri);

        assertEquals(allSlotsUri, existingSlots);
    }

    @Test
    public void putSlotChannelForAlreadyAssociatedSlot() throws SQLException {
        var executionId = "execution_1";

        var userId = "user_1";
        var workflowName = "workflow_1";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        var slotsUri = Set.of("slot_snapshot_1", "slot_snapshot_3");
        var oneMoreSlotUri = Set.of("slot_snapshot_2");
        var allSlotsUri = SetUtils.union(slotsUri, oneMoreSlotUri);

        var channels = Map.of(
            "slot_snapshot_1", "channel_1",
            "slot_snapshot_2", "channel_2",
            "slot_snapshot_3", "channel_3"
        );

        create(userId, workflowName, executionId, storageType, s3Locator);

        executionDao.saveSlots(executionId, allSlotsUri, null);
        executionDao.saveChannels(channels, null);

        Assert.assertThrows(SQLException.class, () ->
            executionDao.saveChannels(Map.of("slot_snapshot_2", "channel_3"), null));

        Map<String, String> channelFromDao = executionDao.findChannels(oneMoreSlotUri);

        assertEquals(1, channelFromDao.size());
        assertEquals("channel_2", channels.get("slot_snapshot_2"));
    }

    @Test
    public void withEmptyArgs() throws SQLException {
        var executionId = "execution_1";

        var userId = "user_1";
        var workflowName = "workflow_1";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        create(userId, workflowName, executionId, storageType, s3Locator);

        executionDao.saveSlots(executionId, Collections.emptySet(), null);
        executionDao.saveChannels(Collections.emptyMap(), null);
        Set<String> existingSlots = executionDao.retainExistingSlots(Collections.emptySet());
        Set<String> nonExistingSlots = executionDao.retainNonExistingSlots(executionId, Collections.emptySet());
        Map<String, String> existingChannels = executionDao.findChannels(Collections.emptySet());

        Assert.assertTrue(existingSlots.isEmpty());
        Assert.assertTrue(nonExistingSlots.isEmpty());
        Assert.assertTrue(existingChannels.isEmpty());
    }

    private String create(String userId, String workflowName, String executionId, String storageType,
                          LMST.StorageConfig storageData) throws SQLException
    {
        executionDao.create(userId, executionId, storageType, storageData, null);
        return workflowDao.upsert(userId, workflowName, executionId, null);
    }
}
