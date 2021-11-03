package ru.yandex.cloud.ml.platform.lzy.backoffice.models;

import io.micronaut.core.annotation.Introspected;
import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Introspected
public class ListUsersResponse {
    List<User> users;

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    public ListUsersResponse(List<User> users) {
        this.users = users;
    }

    public static ListUsersResponse fromModel(BackOffice.ListUsersResponse response){
        return new ListUsersResponse(response.getUsersList().stream().map(User::fromModel).collect(Collectors.toList()));
    }
}
