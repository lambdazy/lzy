# Authentication

### Sandbox sign up

1. Sign in to [ʎzy website](https://lzy.ai) using your [github](https://github.com) account.
2. **[Skip this step if you has already have an RSA key]** Generate RSA keys with openssl:

```shell
$ openssl genrsa -out ~/.ssh/private.pem 2048
$ openssl rsa -in ~/.ssh/private.pem -outform PEM -pubout -out ~/.ssh/public.pem
```

3. Copy content of `~/.ssh/public.pem` to an [add key form](http://lzy.ai/keys).

### Authentication

There are several ways to authenticate in ʎzy:

1. Set ENV variables `$LZY_USER=<your github login>` and `$LZY_KEY_PATH=<path to the private key>`
2. Call `lzy_auth` function:
   ```python
   from lzy.api.v1 import lzy_auth
   lzy_auth(user="<your github login>", key_path="<path to the private key>")
   ```
3. Call `auth` method on a `Lzy` object:
   ```python
   from lzy.api.v1 import Lzy
   lzy = Lzy()
   lzy.auth(user="<your github login>", key_path="<path to the private key>")
   ```

---

In the [**next**](3-basics.md) part we will run our first lzy op.