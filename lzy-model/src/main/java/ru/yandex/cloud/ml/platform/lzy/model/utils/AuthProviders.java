package ru.yandex.cloud.ml.platform.lzy.model.utils;

import yandex.cloud.priv.datasphere.v2.lzy.BackOffice;

public enum AuthProviders {
    GITHUB("github");

    public final String name;

    AuthProviders(String name) {
        this.name = name;
    }

    public static AuthProviders fromGrpcMessage(BackOffice.OAuthProviders provider){
        switch (provider){
            case GITHUB:
                return GITHUB;
            default:
                throw new RuntimeException("Unrecognized option");
        }
    }

    public BackOffice.OAuthProviders toGrpcMessage(){
        switch (this){
            case GITHUB:
                return BackOffice.OAuthProviders.GITHUB;
            default:
                return BackOffice.OAuthProviders.UNRECOGNIZED;
        }
    }

    public static AuthProviders fromString(String s){
        if (s == null){
            return null;
        }
        switch (s){
            case "github":
                return GITHUB;
            default:
                return null;
        }
    }

    public String toString(){
        return name;
    }

}
