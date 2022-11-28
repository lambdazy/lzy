package ai.lzy.allocator.services;

import ai.lzy.allocator.model.debug.InjectedFailures;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
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

    @Post(value = "/allocate-vm", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
    public HttpResponse<String> injectAllocateVmFailure(@Body String body) {
        int n = Integer.parseInt(body);
        if (n > 0 && n < InjectedFailures.FAIL_ALLOCATE_VMS.size()) {
            InjectedFailures.FAIL_ALLOCATE_VMS.get(n).set(vm -> new InjectedFailures.TerminateProcess());
            return HttpResponse.ok("Ok");
        }
        return HttpResponse.badRequest("Index out of range.");
    }

    @Delete(value = "/allocate-vm", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
    public HttpResponse<String> removeAllocateVmFailure(@Body String body) {
        int n = Integer.parseInt(body);
        if (n > 0 && n < InjectedFailures.FAIL_ALLOCATE_VMS.size()) {
            InjectedFailures.FAIL_ALLOCATE_VMS.get(n).set(null);
            return HttpResponse.ok("Ok");
        }
        return HttpResponse.badRequest("Index out of range.");
    }
}
