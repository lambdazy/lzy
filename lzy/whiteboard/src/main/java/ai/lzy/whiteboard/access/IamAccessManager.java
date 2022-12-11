package ai.lzy.whiteboard.access;

import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.util.auth.exceptions.AuthNotFoundException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.util.List;

public class IamAccessManager implements AccessManager {

    private final SubjectServiceClient iamSubjectClient;
    private final AccessBindingClient iamAccessBindingClient;
    private final AccessClient iamAccessClient;

    @Inject
    public IamAccessManager(@Named("WhiteboardIamSubjectClient") SubjectServiceClient subjectClient,
                            @Named("WhiteboardIamAccessBindingClient") AccessBindingClient accessBindingClient,
                            @Named("WhiteboardIamAccessClient") AccessClient iamAccessClient)
    {
        this.iamSubjectClient = subjectClient;
        this.iamAccessBindingClient = accessBindingClient;
        this.iamAccessClient = iamAccessClient;
    }

    @Override
    public void addAccess(String userId, String whiteboardId) {
        // TODO: retries
        final var subj = iamSubjectClient.getSubject(userId);
        iamAccessBindingClient.setAccessBindings(new Whiteboard(whiteboardId),
            List.of(new AccessBinding(Role.LZY_WHITEBOARD_OWNER, subj)));
    }

    @Override
    public boolean checkAccess(String userId, String whiteboardId) {
        // TODO: retries
        final var subj = iamSubjectClient.getSubject(userId);
        try {
            return iamAccessClient.hasResourcePermission(
                subj, new Whiteboard(whiteboardId), AuthPermission.WHITEBOARD_GET);
        } catch (AuthNotFoundException | AuthPermissionDeniedException e) {
            return false;
        }
    }
}
