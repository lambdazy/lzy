package ai.lzy.worker.management;

import ai.lzy.fs.LzyFsServer;
import ai.lzy.fs.fs.LzySlot;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;

import java.util.stream.Collectors;
import java.util.stream.Stream;

@Requires(property = "worker.enable-http-debug", value = "true")
@Controller(value = "/debug", consumes = MediaType.ALL, produces = MediaType.TEXT_PLAIN)
public class ManagementController {
    @Inject
    LzyFsServer lzyFsServer;

    @Get("/slots")
    public String slotsStates() {
        Stream<LzySlot> slots = lzyFsServer.getSlotsManager().slots();

        if (slots.findAny().isEmpty()) {
            return "No opened slots";
        }

        return slots
            .map(slot -> String.format("%s: %s", slot.name(), slot.state()))
            .collect(Collectors.joining(";\n"));
    }

    @Get("/slot")
    public String slotState(@QueryValue("task") String task, @QueryValue("slot") String slot) {
        LzySlot lzySlot = lzyFsServer.getSlotsManager().slot(task, slot);

        if (lzySlot == null) {
            return String.format("No slot found for task %s, slot %s", task, slot);
        }
        return lzySlot.toString();
    }
}
