-- Generate RSA keys
--
-- $ openssl genrsa -out '/tmp/private.pem' 2048
-- $ openssl rsa -in '/tmp/private.pem' -outform PEM -pubout -out '/tmp/public.pem'
--

BEGIN;

INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
VALUES ('lzy-admin', 'INTERNAL', 'lzy-admin', 'ACCESS_ALLOWED', 'USER', 'lzy-admin-hash');

INSERT INTO credentials (name, value, user_id, type)
VALUES ('main', '${public_key}', 'lzy-admin', 'PUBLIC_KEY');

INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
VALUES ('lzy-admin', 'lzy.resource.root', 'root', 'lzy.internal.admin');

COMMIT;
