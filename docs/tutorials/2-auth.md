# Authentication & running terminal

### Sandbox sign up

1. Sign in to [ʎzy website](https://lzy.ai) using your [github](https://github.com) account.
2. **[Skip this step if you has already have an RSA key]** Generate RSA keys with openssl:

```shell
$ openssl genrsa -out ~/.ssh/private.pem 2048
$ openssl rsa -in ~/.ssh/private.pem -outform PEM -pubout -out ~/.ssh/public.pem
```

3. Copy content of `~/.ssh/public.pem` to an [add key form](http://lzy.ai/keys).

### Running terminal

`lzy-terminal -u <github username> -k ~/.ssh/private.pem`

NOTE: github username is CASE SENSITIVE!

If you see `Started terminal` message in stdout - ʎzy is ready for work. Now you can run a workflow!

---

In the [**next**](3-basics.md) part we will run our first lzy op.