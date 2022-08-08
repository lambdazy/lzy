package ai.lzy.iam.resources.subjects;

public record User(String id) implements Subject {
    @Override
    public SubjectType type() {
        return SubjectType.USER;
    }
}
