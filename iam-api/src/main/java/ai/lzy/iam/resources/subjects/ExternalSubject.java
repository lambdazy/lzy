package ai.lzy.iam.resources.subjects;

public interface ExternalSubject<ProviderSubjectT> extends Subject {
    ProviderSubjectT provided();

    @Override
    default String str() {
        return type().name() + '(' + provided() + ')';
    }
}
