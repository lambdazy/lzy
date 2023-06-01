package ai.lzy.worker.management;

import ai.lzy.fs.fs.LzySlot;
import ai.lzy.worker.WorkerApiImpl;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

@Requires(property = "worker.enable-http-debug", value = "true")
@Controller(value = "/debug", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
public class ManagementController {

    @Inject
    private WorkerApiImpl workerApiImpl;

    @Get("/slots")
    public String slotsStates() {
        List<LzySlot> slots = workerApiImpl.lzyFs().getSlotsManager().slots().toList();

        if (slots.isEmpty()) {
            return "No opened slots";
        }

        return slots.stream()
            .map(slot -> String.format("%s: %s", slot.name(), slot.state()))
            .collect(Collectors.joining(";\n"));
    }

    @Get("/slot")
    public String slotState(@QueryValue("task") String task, @QueryValue("slot") String slot) {
        LzySlot lzySlot = workerApiImpl.lzyFs().getSlotsManager().slot(task, slot);

        if (lzySlot == null) {
            return "No slot found for task %s, slot %s".formatted(task, slot);
        }
        return lzySlot.toString();
    }
}
