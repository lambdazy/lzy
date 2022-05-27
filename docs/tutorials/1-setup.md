## Setup

### Requirements
* Fuse
  * Ubuntu: `apt install fuse`
  * macOS: [macFuse](https://osxfuse.github.io)
* Java (>= 17)
  * Ubuntu: `apt install default-jre`
  * macOS: `brew install java`

### Installation

`pip install pylzy`

### Sign Up

1. Sign in to [ʎzy website](https://lzy.ai) using your [github](https://github.com) account.
2. **[Skip this step if you has already have an RSA key]** Generate RSA keys with openssl:
```shell
$ openssl genrsa -out ~/.ssh/private.pem 2048
$ openssl rsa -in ~/.ssh/private.pem -outform PEM -pubout -out ~/.ssh/public.pem
```
3. Copy content of `~/.ssh/public.pem` to an [add key form](http://lzy.ai/keys).

### Running terminal

`lzy-terminal -u <github username> -k ~/.ssh/private.pem`

If you see `Started terminal` message in stdout - lzy is ready for work. Now you can run a python code on the already deployed cluster.

---

In the [**next**](2-basics.md) part we will run our first lzy op.