package ru.yandex.qe.s3.ttl;

import jakarta.annotation.Nonnull;

import java.util.List;
import javax.ws.rs.*;

/**
 * @author nkey
 * @since 28.01.2015
 */

@Path("/ttl/register")
@Produces(MediaTypeConstants.APPLICATION_JSON_WITH_UTF)
@Consumes(MediaTypeConstants.APPLICATION_JSON_WITH_UTF)
public interface TTLRegister {

    @POST
    @Path("/key")
    public List<TTLRecord> add(@QueryParam("s3-type") String s3Type, @QueryParam("s3-endpoint") String s3Endpoint,
        @QueryParam("bucket") @Nonnull String bucket,
        @QueryParam("key") @Nonnull String key,
        @QueryParam("seconds-to-live") long secondsToLive);

    @POST
    @Path("/keys")
    public List<TTLRecord> add(@QueryParam("s3-type") String s3Type, @QueryParam("s3-endpoint") String s3Endpoint,
        @QueryParam("bucket") @Nonnull String bucket,
        @QueryParam("seconds-to-live") long secondsToLive,
        @Nonnull List<String> keys);

    @PUT
    @Path("/key")
    public List<TTLRecord> update(@QueryParam("s3-type") String s3Type, @QueryParam("s3-endpoint") String s3Endpoint,
        @QueryParam("bucket") @Nonnull String bucket,
        @QueryParam("key") @Nonnull String key,
        @QueryParam("seconds-to-live") long secondsToLive);

    @PUT
    @Path("/keys")
    public List<TTLRecord> update(@QueryParam("s3-type") String s3Type, @QueryParam("s3-endpoint") String s3Endpoint,
        @QueryParam("bucket") @Nonnull String bucket,
        @QueryParam("seconds-to-live") long secondsToLive,
        @Nonnull List<String> keys);

    @DELETE
    @Path("/key")
    public List<TTLRecord> remove(@QueryParam("s3-type") String s3Type, @QueryParam("s3-endpoint") String s3Endpoint,
        @QueryParam("bucket") @Nonnull String bucket, @QueryParam("key") String key);

    @DELETE
    @Path("/keys")
    public List<TTLRecord> remove(@QueryParam("s3-type") String s3Type, @QueryParam("s3-endpoint") String s3Endpoint,
        @QueryParam("bucket") @Nonnull String bucket, @Nonnull List<String> keys);

}
