package ai.lzy.allocator.services;

import ai.lzy.allocator.model.debug.InjectedFailures;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.allocator.model.debug.InjectedFailures.FAIL_ALLOCATE_VMS;
import static ai.lzy.allocator.model.debug.InjectedFailures.FAIL_CLONE_DISK;
import static ai.lzy.allocator.model.debug.InjectedFailures.FAIL_CREATE_DISK;

@Controller(value = "/debug/inject-failure", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
public class InjectedFailuresController {

    private static final String ALLOCATE_VM = "/allocate-vm";
    private static final String CREATE_DISK = "/create-disk";
    private static final String CLONE_DISK = "/clone-disk";

    @Get(ALLOCATE_VM)
    public String listAllocateVmFailures() {
        return listImpl("AllocateVM", FAIL_ALLOCATE_VMS);
    }

    @Post(ALLOCATE_VM)
    public HttpResponse<String> injectAllocateVmFailure(@Body String body) {
        return setImpl(Integer.parseInt(body), FAIL_ALLOCATE_VMS, vm -> new InjectedFailures.TerminateProcess());
    }

    @Delete(ALLOCATE_VM)
    public HttpResponse<String> removeAllocateVmFailure(@Body String body) {
        return deleteImpl(Integer.parseInt(body), FAIL_ALLOCATE_VMS);
    }

    @Get(CREATE_DISK)
    public String listCreateDiskFailures() {
        return listImpl("CreateDisk", FAIL_CREATE_DISK);
    }

    @Post(CREATE_DISK)
    public HttpResponse<String> injectCreateDiskFailure(@Body String body) {
        return setImpl(Integer.parseInt(body), FAIL_CREATE_DISK, InjectedFailures.TerminateProcess::new);
    }

    @Delete(CREATE_DISK)
    public HttpResponse<String> removeCreateDiskFailure(@Body String body) {
        return deleteImpl(Integer.parseInt(body), FAIL_CREATE_DISK);
    }

    @Get(CLONE_DISK)
    public String listCloneDiskFailures() {
        return listImpl("CloneDisk", FAIL_CLONE_DISK);
    }

    @Post(CLONE_DISK)
    public HttpResponse<String> injectCloneDiskFailure(@Body String body) {
        return setImpl(Integer.parseInt(body), FAIL_CLONE_DISK, InjectedFailures.TerminateProcess::new);
    }

    @Delete(CLONE_DISK)
    public HttpResponse<String> removeCloneDiskFailure(@Body String body) {
        return deleteImpl(Integer.parseInt(body), FAIL_CLONE_DISK);
    }

    private static <T> String listImpl(String name, List<AtomicReference<T>> list) {
        var sb = new StringBuilder();
        for (int i = 0; i < list.size(); ++i) {
            sb.append("%s Failure #%02d: %s\n".formatted(name, i, list.get(i).get() != null ? "ON" : "OFF"));
        }
        return sb.toString();
    }

    private static <T> HttpResponse<String> setImpl(int n, List<AtomicReference<T>> list, T value) {
        if (n >= 0 && n < list.size()) {
            list.get(n).set(value);
            return HttpResponse.ok("Ok");
        }
        return HttpResponse.badRequest("Index out of range.");
    }

    private static <T> HttpResponse<String> deleteImpl(int n, List<AtomicReference<T>> list) {
        if (n >= 0 && n < list.size()) {
            list.get(n).set(null);
            return HttpResponse.ok("Ok");
        }
        return HttpResponse.badRequest("Index out of range.");
    }
}
