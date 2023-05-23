package ai.lzy.iam.resources.subjects;

public record External(String id, String details) implements Subject {
    @Override
    public SubjectType type() {
        return SubjectType.EXTERNAL;
    }

    @Override
    public String str() {
        return "External(" + id() + ", " + details + ')';
    }
}
