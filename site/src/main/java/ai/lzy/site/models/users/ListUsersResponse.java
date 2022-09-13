package ai.lzy.site.models.users;

import ai.lzy.v1.deprecated.BackOffice;
import io.micronaut.core.annotation.Introspected;

import java.util.List;
import java.util.stream.Collectors;

@Introspected
public class ListUsersResponse {

    List<User> users;

    public ListUsersResponse(List<User> users) {
        this.users = users;
    }

    public static ListUsersResponse fromModel(BackOffice.ListUsersResponse response) {
        return new ListUsersResponse(
            response.getUsersList().stream().map(User::fromModel).collect(Collectors.toList()));
    }

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }
}
