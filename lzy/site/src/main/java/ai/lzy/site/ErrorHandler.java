package ai.lzy.site;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.http.server.exceptions.response.ErrorContext;
import io.micronaut.http.server.exceptions.response.ErrorResponseProcessor;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

@Produces
@Singleton
@Requires(classes = {Exception.class, ExceptionHandler.class})
public class ErrorHandler implements ExceptionHandler<Exception, HttpResponse<?>> {
    private static final Logger LOG = LogManager.getLogger(ErrorHandler.class);

    private final ErrorResponseProcessor<?> errorResponseProcessor;

    public ErrorHandler(ErrorResponseProcessor<?> errorResponseProcessor) {
        this.errorResponseProcessor = errorResponseProcessor;
    }

    @Override
    public HttpResponse<?> handle(HttpRequest request, Exception exception) {
        var errorId = UUID.randomUUID().toString();
        LOG.error("Error while processing request to {}:\nrequest: {}\n error_id: {}\n error: ",
            request.getPath(), errorId, exception);

        return errorResponseProcessor.processResponse(ErrorContext.builder(request)
            .errorMessage(String.format("""
                Some internal error happened while processing request to %s.
                Try again later. To get help, say to LZY team that you have error with id %s""",
                request.getPath(), errorId))
            .build(), HttpResponse.serverError());
    }
}
