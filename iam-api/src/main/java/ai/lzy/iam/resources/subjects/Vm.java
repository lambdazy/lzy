package ai.lzy.iam.resources.subjects;

public record Vm(String id) implements Subject {
    @Override
    public SubjectType type() {
        return SubjectType.VM;
    }
}
