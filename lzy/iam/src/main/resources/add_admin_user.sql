-- Generate RSA keys
--
-- $ openssl genrsa -out '/tmp/private.pem' 2048
-- $ openssl rsa -in '/tmp/private.pem' -outform PEM -pubout -out '/tmp/public.pem'
--

BEGIN;

INSERT INTO users (user_id, auth_provider, provider_user_id, access_type, user_type, request_hash)
VALUES ('lzy-admin', 'INTERNAL', 'lzy-admin', 'ACCESS_ALLOWED', 'USER', 'lzy-admin-hash');

INSERT INTO credentials (name, value, user_id, type)
VALUES ('main', '-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwr34TfgVtV8emOfwIIZG
LDwBsy76jr1DCCYobrR5si5l7b12KkyRn/DBTLkNoyfw0BMAx2+SvuGZPz8SqR88
qCNNZjX/g66NnRNoNX5mm/cnAT/0OsxIIwGKvaNM1BE7rasIJ3wv9RDP2EaTIuAJ
XIZa4iGa4Za55NZAV2YOZf4yZilWZosdnzO87QR7xDvkCWjgG72FCdM/dpfge+mp
wm4YyIvyole/yoVpF0zHUXFl3zb7RNi2kKxBL5PjCASHeGOrFYekmKlCAjXGyl/E
C+RElwiBEis5XSHsjm6l3c16uxBnlHi+zXprRvB8vWy6XAjrQxxIXUi7C9od6/kV
AQIDAQAB
-----END PUBLIC KEY-----', 'lzy-admin', 'PUBLIC_KEY');

INSERT INTO user_resource_roles (user_id, resource_id, resource_type, role)
VALUES ('lzy-admin', 'lzy.resource.root', 'root', 'lzy.internal.admin');

COMMIT;
