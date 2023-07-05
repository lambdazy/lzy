package ai.lzy.service.operations;

import ai.lzy.service.WithoutWbAndSchedulerLzyContextTests;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static ai.lzy.service.IamUtils.authorize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AgainstResourceLeaksTests extends WithoutWbAndSchedulerLzyContextTests {
    private volatile Set<String> allocatorSessions;
    private volatile Set<String> allocatePortalVms;
    private volatile Set<String> portalIamSubjects;

    private OldScenarios oldScenarios;

    @Before
    public void before() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InterruptedException {
        allocatorSessions = ConcurrentHashMap.newKeySet();
        allocatePortalVms = ConcurrentHashMap.newKeySet();
        portalIamSubjects = ConcurrentHashMap.newKeySet();

        oldScenarios = new OldScenarios(authorize(lzyClient, "test-user-1", iamClient), lzyPrivateClient);

        allocator()
            .onCreateSession(allocatorSessions::add)
            .onDeleteSession(allocatorSessions::remove)
            .onAllocate(allocatePortalVms::add)
            .onFree(allocatePortalVms::remove);
        iamSubjectsService()
            .onCreate(portalIamSubjects::add)
            .onRemove(portalIamSubjects::remove);

        oldScenarios
            .setAssertBeforeStartWorkflow(() -> {
                assertTrue(allocatorSessions.isEmpty());
                assertTrue(allocatePortalVms.isEmpty());
                assertTrue(portalIamSubjects.isEmpty());
            })
            .setAssertAfterStartWorkflow(() -> {
                assertFalse(allocatorSessions.isEmpty());
                assertFalse(allocatePortalVms.isEmpty());
                assertFalse(portalIamSubjects.isEmpty());
            })
            .setAssertAfterStopWorkflow(() -> {
                assertTrue(allocatorSessions.isEmpty());
                assertTrue(allocatePortalVms.isEmpty());
                assertTrue(portalIamSubjects.isEmpty());
            });
    }

    @Test
    public void releaseResourcesInFinishWorkflow() {
        oldScenarios.startAndFinishExecution();
    }

    @Test
    public void releaseResourcesInAbortWorkflow() {
        oldScenarios.startAndAbortExecution();
    }

    @Test
    public void releaseResourcesInPrivateAbortWorkflow() {
        oldScenarios.startAndPrivateAbortExecution();
    }

    @Test
    public void releaseResourceInActiveExecutionsClash() {
        oldScenarios.activeExecutionsClash();
    }
}
