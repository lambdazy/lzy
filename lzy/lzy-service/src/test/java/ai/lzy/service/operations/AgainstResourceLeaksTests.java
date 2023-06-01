package ai.lzy.service.operations;

import ai.lzy.service.ContextAwareTests;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AgainstResourceLeaksTests extends ContextAwareTests {
    private Set<String> allocatorSessions;
    private Set<String> allocatePortalVms;
    private Set<String> portalIamSubjects;

    private OldScenarios oldScenarios;

    @Before
    public void setUp() {
        allocatorSessions = ConcurrentHashMap.newKeySet();
        allocatePortalVms = ConcurrentHashMap.newKeySet();
        portalIamSubjects = ConcurrentHashMap.newKeySet();

        oldScenarios = new OldScenarios(authLzyGrpcClient, authLzyPrivateGrpcClient);

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
