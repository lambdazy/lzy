package ai.lzy.whiteboard.access;

public interface AccessManager {

    void addAccess(String userId, String whiteboardId);

    boolean checkAccess(String userId, String whiteboardId);

}
