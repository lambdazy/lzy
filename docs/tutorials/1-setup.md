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
<p>

python                         3.6.2

python                         3.6.3

python                         3.6.4

python                         3.6.5

python                         3.6.6

python                         3.6.7

python                         3.6.8

python                         3.6.9

python                        3.6.10

python                        3.6.12

python                        3.6.13

python                         3.7.0

python                         3.7.1

python                         3.7.2

python                         3.7.3

python                         3.7.4

python                         3.7.5

python                         3.7.6

python                         3.7.7

python                         3.7.9

python                        3.7.10

python                        3.7.11

python                        3.7.13

python                         3.8.0

python                         3.8.1

python                         3.8.2

python                         3.8.3

python                         3.8.5

python                         3.8.8

python                        3.8.10

python                        3.8.11

python                        3.8.12

python                        3.8.13

python                         3.9.0

python                         3.9.1

python                         3.9.2

python                         3.9.4

python                         3.9.5

python                         3.9.6

python                         3.9.7

python                        3.9.11

python                        3.9.12

python                        3.10.0

python                        3.10.3

python                        3.10.4





</p>
</details>


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
