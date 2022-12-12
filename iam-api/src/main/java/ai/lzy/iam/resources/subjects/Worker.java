package ai.lzy.iam.resources.subjects;

public record Worker(String id) implements Subject {
    @Override
    public SubjectType type() {
        return SubjectType.WORKER;
    }
}
