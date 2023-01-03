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

        workflowDao.create(firstExecutionId, userId, firstWorkflowName, storageType, s3Locator);
        workflowDao.create(secondExecutionId, userId, secondWorkflowName, storageType, s3Locator);

        executionDao.saveSlots(firstExecutionId, firstSlotsUri);
        executionDao.saveSlots(secondExecutionId, secondSlotsUri);

        Set<String> existingSlots = executionDao.retainExistingSlots(allSlotsUri);

        Set<String> nonExistForFirst = executionDao.retainNonExistingSlots(firstExecutionId, firstSlotsUri);
        Set<String> allSecondNonExistForFirst = executionDao.retainNonExistingSlots(firstExecutionId, secondSlotsUri);

        Set<String> nonExistForSecond = executionDao.retainNonExistingSlots(secondExecutionId, secondSlotsUri);
        Set<String> allFirstNonExistForSecond = executionDao.retainNonExistingSlots(secondExecutionId, firstSlotsUri);

        Assert.assertEquals(allSlotsUri, existingSlots);
        Assert.assertTrue(nonExistForFirst.isEmpty());
        Assert.assertEquals(secondSlotsUri, allSecondNonExistForFirst);
        Assert.assertTrue(nonExistForSecond.isEmpty());
        Assert.assertEquals(firstSlotsUri, allFirstNonExistForSecond);
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

        workflowDao.create(firstExecutionId, userId, firstWorkflowName, storageType, s3Locator);
        workflowDao.create(secondExecutionId, userId, secondWorkflowName, storageType, s3Locator);

        executionDao.saveSlots(firstExecutionId, firstSlotsUri);
        executionDao.saveSlots(secondExecutionId, secondSlotsUri);

        executionDao.saveChannels(channels);

        Map<String, String> firstChannelsFromDao = executionDao.findChannels(firstSlotsUri);
        Map<String, String> secondChannelsFromDao = executionDao.findChannels(secondSlotsUri);

        Assert.assertEquals(channels, firstChannelsFromDao);
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

        workflowDao.create(firstExecutionId, userId, firstWorkflowName, storageType, s3Locator);

        var slotsUri = Set.of("slot_snapshot_1", "slot_snapshot_2");

        Assert.assertThrows(SQLException.class, () -> executionDao.saveSlots(secondExecutionId, slotsUri));
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

        workflowDao.create(executionId, userId, workflowName, storageType, s3Locator);
        executionDao.saveSlots(executionId, slotsUri);

        Assert.assertThrows(SQLException.class, () -> executionDao.saveChannels(channels));
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

        workflowDao.create(executionId, userId, firstWorkflowName, storageType, s3Locator);
        executionDao.saveSlots(executionId, allSlotsUri);

        Assert.assertThrows(SQLException.class, () -> executionDao.saveSlots(executionId, oneMoreSlotUri));

        Set<String> existingSlots = executionDao.retainExistingSlots(allSlotsUri);

        Assert.assertEquals(allSlotsUri, existingSlots);
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

        workflowDao.create(executionId, userId, workflowName, storageType, s3Locator);
        executionDao.saveSlots(executionId, allSlotsUri);
        executionDao.saveChannels(channels);

        Assert.assertThrows(SQLException.class, () ->
            executionDao.saveChannels(Map.of("slot_snapshot_2", "channel_3")));

        Map<String, String> channelFromDao = executionDao.findChannels(oneMoreSlotUri);

        Assert.assertEquals(1, channelFromDao.size());
        Assert.assertEquals("channel_2", channels.get("slot_snapshot_2"));
    }

    @Test
    public void withEmptyArgs() throws SQLException {
        var executionId = "execution_1";

        var userId = "user_1";
        var workflowName = "workflow_1";
        var storageType = "USER";
        var s3Locator = LMST.StorageConfig.getDefaultInstance();

        workflowDao.create(executionId, userId, workflowName, storageType, s3Locator);

        executionDao.saveSlots(executionId, Collections.emptySet());
        executionDao.saveChannels(Collections.emptyMap());
        Set<String> existingSlots = executionDao.retainExistingSlots(Collections.emptySet());
        Set<String> nonExistingSlots = executionDao.retainNonExistingSlots(executionId, Collections.emptySet());
        Map<String, String> existingChannels = executionDao.findChannels(Collections.emptySet());

        Assert.assertTrue(existingSlots.isEmpty());
        Assert.assertTrue(nonExistingSlots.isEmpty());
        Assert.assertTrue(existingChannels.isEmpty());
    }

    @After
    public void tearDown() {
        lzyStorageCtx.stop();
    }
}
