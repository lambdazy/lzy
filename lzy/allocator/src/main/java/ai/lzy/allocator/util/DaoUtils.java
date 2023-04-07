package ai.lzy.allocator.util;

public final class DaoUtils {
    private DaoUtils() {
    }

    public static String generateNParamArray(int size) {
        if (size <= 0) {
            return "()";
        }
        var sb = new StringBuilder(size * 2 + 1);
        sb.append("(");
        for (int i = 0; i < size; i++) {
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        return sb.toString();
    }
}
