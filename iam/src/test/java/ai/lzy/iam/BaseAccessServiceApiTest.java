package ai.lzy.iam;

import ai.lzy.iam.resources.*;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.util.auth.exceptions.AuthBadRequestException;
import ai.lzy.v1.iam.IAM;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public abstract class BaseAccessServiceApiTest {
    public static final Logger LOG = LogManager.getLogger(BaseAuthServiceApiTest.class);

    @Test
    public void validAccessUser() {
        validAccess(SubjectType.USER);
    }

    @Test
    public void validAccessServant() {
        validAccess(SubjectType.SERVANT);
    }

    public void validAccess(SubjectType subjectType) {
        String userId = "user1";
        createSubject(userId, "", "", subjectType);
        final Subject user = subject(userId);

        AuthResource whiteboardResource = new Whiteboard("whiteboard");
        setAccessBindings(whiteboardResource, List.of(
                new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user)
        ));
        assertTrue(hasResourcePermission(
                user,
                whiteboardResource,
                AuthPermission.WHITEBOARD_GET)
        );
        assertTrue(hasResourcePermission(
                user,
                whiteboardResource,
                AuthPermission.WHITEBOARD_UPDATE)
        );
        assertTrue(hasResourcePermission(
                user,
                whiteboardResource,
                AuthPermission.WHITEBOARD_CREATE)
        );
        assertTrue(hasResourcePermission(
                user,
                whiteboardResource,
                AuthPermission.WHITEBOARD_DELETE)
        );

        AuthResource workflowResource = new Workflow("workflow");
        List<AccessBinding> workflowAccessBinding = List.of(
                new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user)
        );
        setAccessBindings(workflowResource, workflowAccessBinding);
        assertTrue(hasResourcePermission(
                user,
                workflowResource,
                AuthPermission.WORKFLOW_RUN)
        );
        assertTrue(hasResourcePermission(
                user,
                workflowResource,
                AuthPermission.WORKFLOW_STOP)
        );
        assertTrue(hasResourcePermission(
                user,
                workflowResource,
                AuthPermission.WORKFLOW_DELETE)
        );
        assertTrue(hasResourcePermission(
                user,
                workflowResource,
                AuthPermission.WORKFLOW_GET)
        );
        assertEquals(
                workflowAccessBinding,
                listAccessBindings(workflowResource).collect(Collectors.toList())
        );

        updateAccessBindings(whiteboardResource, List.of(
                new AccessBindingDelta(
                        AccessBindingDelta.AccessBindingAction.REMOVE,
                        new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user))
        ));
        updateAccessBindings(workflowResource, List.of(
                new AccessBindingDelta(
                        AccessBindingDelta.AccessBindingAction.REMOVE,
                        new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user))
        ));
        removeSubject(user);
    }

    @Test
    public void invalidAccessUser() {
        invalidAccess(SubjectType.USER);
    }

    @Test
    public void invalidAccessServant() {
        invalidAccess(SubjectType.SERVANT);
    }

    public void invalidAccess(SubjectType subjectType) {
        String userId = "user1";
        createSubject(userId, "", "", subjectType);
        final Subject user = subject(userId);

        AuthResource whiteboardResource = new Whiteboard("whiteboard");
        setAccessBindings(whiteboardResource, List.of(
                new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user)
        ));
        assertTrue(hasResourcePermission(
                user,
                whiteboardResource,
                AuthPermission.WHITEBOARD_GET)
        );

        updateAccessBindings(whiteboardResource, List.of(
                new AccessBindingDelta(
                        AccessBindingDelta.AccessBindingAction.REMOVE,
                        new AccessBinding(Role.LZY_WHITEBOARD_OWNER.role(), user))
        ));
        try {
            hasResourcePermission(user, whiteboardResource, AuthPermission.WHITEBOARD_GET);
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception::{}", e.getInternalDetails());
        }

        AuthResource workflowResource = new Workflow("workflow");
        setAccessBindings(workflowResource, List.of(
                new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user)
        ));
        assertTrue(hasResourcePermission(
                user,
                workflowResource,
                AuthPermission.WORKFLOW_RUN)
        );

        updateAccessBindings(workflowResource, List.of(
                        new AccessBindingDelta(
                                AccessBindingDelta.AccessBindingAction.REMOVE,
                                new AccessBinding(Role.LZY_WORKFLOW_OWNER.role(), user))
                )
        );
        try {
            hasResourcePermission(user, workflowResource, AuthPermission.WORKFLOW_RUN);
            fail();
        } catch (AuthBadRequestException e) {
            LOG.info("Valid exception::{}", e.getInternalDetails());
        }

        removeSubject(user);
    }
    protected abstract Subject subject(String id);

    protected abstract void createSubject(String id, String name, String value, SubjectType subjectType);

    protected abstract void removeSubject(Subject subject);

    protected abstract void setAccessBindings(AuthResource resource, List<AccessBinding> accessBinding);

    protected abstract Stream<AccessBinding> listAccessBindings(AuthResource resource);

    protected abstract void updateAccessBindings(AuthResource resource, List<AccessBindingDelta> accessBinding);

    protected abstract boolean hasResourcePermission(Subject subject, AuthResource resource, AuthPermission permission);
}
