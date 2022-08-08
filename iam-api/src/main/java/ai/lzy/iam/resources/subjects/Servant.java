package ai.lzy.iam.resources.subjects;

public record Servant(String id) implements Subject {
    @Override
    public SubjectType type() {
        return SubjectType.SERVANT;
    }
}
