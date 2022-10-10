package ai.lzy.iam.resources.subjects;

public interface Subject {
    String id();
    SubjectType type();

    default String str() {
        return type().name() + '(' + id() + ')';
    }
}
