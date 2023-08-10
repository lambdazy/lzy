package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelDao;
import ai.lzy.channelmanager.db.PeerDao;
import ai.lzy.channelmanager.db.TransferDao;
import ai.lzy.channelmanager.model.Peer;
import ai.lzy.v1.common.LC;
import ai.lzy.v1.common.LMD;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.sql.SQLException;

import static ai.lzy.channelmanager.db.TransferDao.State.PENDING;
import static ai.lzy.model.db.test.DatabaseTestUtils.preparePostgresConfig;

public class DbTest {
    @ClassRule
    public static final PreparedDbRule channelManagerDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    public static ApplicationContext context;
    private static PeerDao peerDao;
    private static ChannelDao channelDao;
    private static TransferDao transferDao;

    @BeforeClass
    public static void init() {
        var channelManagerDbConfig = preparePostgresConfig("channel-manager", channelManagerDb.getConnectionInfo());
        context = ApplicationContext.run(channelManagerDbConfig);
        peerDao = context.getBean(PeerDao.class);
        channelDao = context.getBean(ChannelDao.class);
        transferDao = context.getBean(TransferDao.class);
    }

    @AfterClass
    public static void after() {
        context.close();
    }

    @Test
    public void channelDaoTestSimple() throws SQLException {
        var ds = LMD.DataScheme.newBuilder()
            .setSchemeContent("Content")
            .setDataFormat("Format")
            .build();

        var channel = channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", ds, null, null, null);
        var channel2 = channelDao.get(channel.id(), null);

        Assert.assertEquals(channel, channel2);

        var channel3 = channelDao.drop(channel.id(), null);
        Assert.assertEquals(channel, channel3);

        var channel4 = channelDao.get(channel.id(), null);
        Assert.assertNull(channel4);

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void channelDaoFindTest() throws SQLException {
        var ds = LMD.DataScheme.newBuilder()
            .setSchemeContent("Content")
            .setDataFormat("Format")
            .build();

        var channelWithProducer = channelDao.create(
            "test-channel", "test-user-id", "exec-id", "wfName", ds, "producerUri", null, null);

        var channel = channelDao.find("test-user-id", "exec-id", "producerUri", null, null);

        Assert.assertEquals(channelWithProducer, channel);
        Assert.assertNull(channelDao.find("test-user-id", "exec-id", "producerUri", "consumerUri", null));
        Assert.assertNull(channelDao.find("test-user-id", "exec-id", null, "consumerUri", null));
        Assert.assertNull(channelDao.find("test-user-id", "exec-id", null, null, null));

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void channelDaoDropAllTest() throws SQLException {
        var ds = LMD.DataScheme.newBuilder()
            .setSchemeContent("Content")
            .setDataFormat("Format")
            .build();

        var channelWithProducer = channelDao.create(
            "test-channel", "test-user-id", "exec-id", "wfName", ds, "producerUri", null, null);
        channelDao.create(
            "test-channel-2", "test-user-id", "exec-id", "wfName", ds, "producerUri", "consumerUri", null);

        channelDao.dropAll("exec-id", null);

        Assert.assertNull(channelDao.get(channelWithProducer.id(), null));
        Assert.assertNull(channelDao.get("test-channel-2", null));

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void channelDaoListTest() throws SQLException {
        var ds = LMD.DataScheme.newBuilder()
            .setSchemeContent("Content")
            .setDataFormat("Format")
            .build();

        var channel = channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", ds, null, null, null);

        var producer = LC.PeerDescription.newBuilder()
            .setPeerId("producer")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("producer")
                .build())
            .build();

        var consumer = LC.PeerDescription.newBuilder()
            .setPeerId("consumer")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("consumer")
                .build())
            .build();

        var peerProd = peerDao.create("test-channel", producer, Peer.Role.PRODUCER, PeerDao.Priority.PRIMARY, false,
            "idk-1", "some_hash", null);

        var peerCons = peerDao.create("test-channel", consumer, Peer.Role.CONSUMER, PeerDao.Priority.PRIMARY, false,
            "idk-2", "some_hash", null);

        var channelStatus = channelDao.list("exec-id", null, null).get(0);

        Assert.assertEquals(channel, channelStatus.channel());
        Assert.assertEquals(producer, channelStatus.producers().get(0));
        Assert.assertEquals(consumer, channelStatus.consumers().get(0));

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void peerDaoTestSimple() throws SQLException {
        channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", null, null, null, null);

        var peer1 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer1")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer1")
                .build())
            .build(), Peer.Role.PRODUCER, PeerDao.Priority.PRIMARY, false, "idk", "some-hash", null);

        var peer2 = peerDao.get("peer1", "test-channel", null);
        Assert.assertEquals(peer1, peer2);

        var peer3 = peerDao.drop("peer1", "test-channel", null);
        Assert.assertEquals(peer1, peer3);

        var peer4 = peerDao.get("peer1", "test-channel", null);
        Assert.assertNull(peer4);

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void peerDaoTestPriority() throws SQLException {
        var chan = channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", null, null, null, null);

        var peer1 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer1")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer1")
                .build())
            .build(), Peer.Role.PRODUCER, PeerDao.Priority.PRIMARY, false, "idk-1", "req-hash", null);

        var peer2 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer2")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer2")
                .build())
            .build(), Peer.Role.PRODUCER, PeerDao.Priority.PRIMARY, false, "idk-2", "req-hash", null);

        peerDao.decrementPriority(peer2.id(), "test-channel", null);
        var producer = peerDao.findProducer("test-channel", null);

        Assert.assertEquals(peer1, producer);

        peerDao.decrementPriority(peer1.id(), "test-channel", null);
        peerDao.decrementPriority(peer1.id(), "test-channel", null);

        var producer2 = peerDao.findProducer("test-channel", null);
        Assert.assertEquals(peer2, producer2);

        channelDao.drop(chan.id(), null);

        Assert.assertNull(peerDao.get(peer1.id(), "test-channel", null));
        Assert.assertNull(peerDao.get(peer2.id(), "test-channel", null));

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void peerDaoTestConnected() throws SQLException {
        var chan = channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", null, null, null, null);

        var peer1 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer1")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer1")
                .build())
            .build(), Peer.Role.CONSUMER, PeerDao.Priority.PRIMARY, true, "idk-1", "some-hash", null);

        var peer2 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer2")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer2")
                .build())
            .build(), Peer.Role.CONSUMER, PeerDao.Priority.PRIMARY, false, "idk-2", "some-hash", null);

        var peers = peerDao.markConsumersAsConnected("test-channel", "idk-3", "some-hash", null);

        Assert.assertEquals(1, peers.size());
        Assert.assertEquals(peer2, peers.get(0));

        var peers1 = peerDao.markConsumersAsConnected("test-channel", "idk-4", "some-hash", null);
        Assert.assertEquals(0, peers1.size());

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void peerDaoTestTransfer() throws SQLException {
        var chan = channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", null, null, null, null);

        var peer1 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer1")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer1")
                .build())
            .build(), Peer.Role.PRODUCER, PeerDao.Priority.PRIMARY, false, "idk-1", "some-hash", null);

        var peer2 = peerDao.create("test-channel", LC.PeerDescription.newBuilder()
            .setPeerId("peer2")
            .setSlotPeer(LC.PeerDescription.SlotPeer.newBuilder()
                .setPeerUrl("peer2")
                .build())
            .build(), Peer.Role.CONSUMER, PeerDao.Priority.PRIMARY, false, "idk-2", "some-hash", null);

        transferDao.create(peer1.id(), peer2.id(), "test-channel", PENDING, "idk-3", "some-hash", null);

        Assert.assertTrue(transferDao.hasPendingOrActiveTransfers(peer1.id(), "test-channel", null));

        var list = transferDao.listPending(null);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(peer1, list.get(0).from());
        Assert.assertEquals(peer2, list.get(0).to());

        transferDao.markActive(list.get(0).id(), list.get(0).channelId(), "idk-4", null);

        var list1 = transferDao.listPending(null);
        Assert.assertEquals(0, list1.size());

        var transfer = transferDao.get(list.get(0).id(), "test-channel", null);
        Assert.assertEquals(TransferDao.State.ACTIVE, transfer.state());

        transferDao.markCompleted(list.get(0).id(), list.get(0).channelId(), "idk-5", null);
        var transfer1 = transferDao.get(list.get(0).id(), "test-channel", null);
        Assert.assertEquals(TransferDao.State.COMPLETED, transfer1.state());

        transferDao.markFailed(list.get(0).id(), list.get(0).channelId(), "FAILED", "idk-6", null);
        var transfer2 = transferDao.get(list.get(0).id(), "test-channel", null);
        Assert.assertEquals(TransferDao.State.FAILED, transfer2.state());
        Assert.assertEquals("FAILED", transfer2.errorDescription());

        channelDao.dropAll("exec-id", null);
    }

    @Test
    public void listWithoutPeers() throws SQLException {
        var chan = channelDao.create("test-channel", "test-user-id", "exec-id", "wfName", null, null, null, null);

        var list = channelDao.list("exec-id", null, null);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(chan, list.get(0).channel());

        channelDao.dropAll("exec-id", null);
    }
}
