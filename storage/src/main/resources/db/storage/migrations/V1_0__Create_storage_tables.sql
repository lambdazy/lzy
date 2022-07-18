create table yc_s3_credentials (
    user_id varchar(255) primary key,
    service_account varchar(255) not null,
    access_token varchar(255) not null,
    secret_token varchar(255) not null
)
