package ai.lzy.portal.storage;

public class Utils {
    public static String removeLeadingSlash(String path) {
        if (path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }
}
