package ai.lzy.model;

public class EnvironmentInstallationException extends Exception {
    public EnvironmentInstallationException(String message) {
        super(message);
    }

    public EnvironmentInstallationException(Exception e) {
        super(e);
    }
}
