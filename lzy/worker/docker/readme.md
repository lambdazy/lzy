# Docker images description

## Worker image

System image, contains worker application.

Lzy supports image with custom user environment, from which worker creates docker container. 
Lzy use docker-in-docker scheme here. So **worker image** contains docker.

Worker has an alternative for docker-in-docker scheme. If user has no custom environment,
worker can execute tasks in its own environment. So **worker image** contains pylzy python package.

**Worker image** split on **[base](System.Base.Dockerfile)** and **[final](System.Dockerfile)** images.
**Base** image contains all dependencies (GPU drivers, java, docker, conda, etc.) and updates rarely.
**Final** image contains only pylzy python package and worker application, updates with every worker or pylzy update.

(Suggestion: rename to _Worker.Dockerfile_)

## Default user image

Contains GPU drivers, conda environments with different versions of python, and pylzy package in all environments.
Can be used as default user environment. User should use it as parent image for their image with custom environment.

**Default user image** split on **[base](DefaultEnv.Base.Dockerfile)** and **[final](DefaultEnv.Dockerfile)** images.
**Base** image contains all dependencies (GPU drivers, conda, etc.) and updates rarely.
**Final** image contains only pylzy python package and updates with every pylzy update.

(Suggestion: rename to _UserDefault.Dockerfile_)

## Test user image

Light version of **default user image**. Used in tests.
It contains only py39 conda-env, and has no GPU drivers.

Also split on **[base](TestEnv.Base.Dockerfile)** and **[final](TestEnv.Dockerfile)** images.

(Suggestion: rename to _UserTest.Dockerfile_)
