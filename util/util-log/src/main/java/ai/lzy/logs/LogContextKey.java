package ai.lzy.logs;

public final class LogContextKey {

    public static final String REQUEST_ID = "rid";
    public static final String EXECUTION_ID = "exec_id";
    public static final String EXECUTION_TASK_ID = "exec_tid";

    public static final String SUBJECT = "subj";

    public static final String ACTION = "action";

    public static final String OPERATION_ID = "operation_id";
    public static final String VM_ID = "vm_id";
    public static final String DYNAMIC_MOUNT_ID = "dynamic_mount_id";


    private LogContextKey() { }
}
