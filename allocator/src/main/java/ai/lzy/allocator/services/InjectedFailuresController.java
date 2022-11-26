package ai.lzy.allocator.services;

import ai.lzy.allocator.model.debug.InjectedFailures;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;

@Controller(value = "/debug/inject-failure")
public class InjectedFailuresController {

    @Get(value = "/allocate-vm", produces = MediaType.TEXT_PLAIN)
    public String listAllocateVmFailures() {
        var sb = new StringBuilder();
        for (int i = 0; i < InjectedFailures.FAIL_ALLOCATE_VMS.size(); ++i) {
            sb.append("AllocateVm Failure #%02d: %s\n"
                .formatted(i, InjectedFailures.FAIL_ALLOCATE_VMS.get(i).get() != null ? "ON" : "OFF"));
        }
        return sb.toString();
    }

    @Post(value = "/allocate-vm", consumes = MediaType.TEXT_PLAIN, produces = MediaType.TEXT_PLAIN)
    public HttpResponse<String> injectAllocateVmFailure(int n) {
        if (n > 0 && n < InjectedFailures.FAIL_ALLOCATE_VMS.size()) {
            InjectedFailures.FAIL_ALLOCATE_VMS.get(n).set(vm -> new InjectedFailures.TerminateProcess());
            return HttpResponse.ok();
        }
        return HttpResponse.badRequest("Index out of range.");
    }
}
