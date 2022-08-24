## Setup

### Requirements
* Fuse
  * Ubuntu: `apt install fuse`
  * macOS: [macFuse](https://osxfuse.github.io)
* Java (>= 17)
  * Ubuntu: `apt install default-jre`
  * macOS: `brew install java`
* Python
<details><summary>Supported python versions</summary>

Python 3.6: [3.6.2 - 3.6.10], 3.6.12, 3.6.13

Python 3.7: [3.7.0 - 3.7.7], 3.7.9, 3.7.10, 3.7.11, 3.7.13

Python 3.8: [3.8.0 - 3.8.3], 3.8.5, 3.8.8, [3.8.10 - 3.8.13]

Python 3.9: [3.9.0 - 3.9.2], [3.9.4 - 3.9.7], 3.9.11, 3.9.12

Python 3.10: 3.10.0, 3.10.3, 3.10.4

</details>


### Installation

[![Pypi version](https://img.shields.io/pypi/v/pylzy)](https://pypi.org/project/pylzy/)

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
