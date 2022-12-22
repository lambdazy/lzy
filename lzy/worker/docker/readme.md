# Docker images description

## Worker image

System image, contains worker application.

Lzy supports images with custom user environment. Worker implements 
[docker-in-docker](https://www.docker.com/blog/docker-can-now-run-within-docker/) scheme here:

* There is an installed [Docker](https://docs.docker.com/get-started/overview/) inside **worker image**
* When worker container creates, it runs Docker daemon, 
which will listen for Docker API requests and will manage Docker objects.
* When worker going to execute task, it runs inner Docker container with custom environment from user image. 
Task will be executed inside inner container.

Worker has an alternative for docker-in-docker scheme. 
If user has no custom environment, worker can execute tasks in its own environment in outer container. 
So **worker image** contains pylzy python package.

**Worker image** split on **[base](System.Base.Dockerfile)** and **[final](System.Dockerfile)** images.
* **Base** image contains all dependencies (GPU drivers, java, docker, conda, etc.) and updates rarely.
* **Final** image extends **base** image with worker application and python packages (including pylzy).
It updates with every worker or pylzy update.

(Suggestion: rename to _Worker.Dockerfile_)

## Default user image

Contains GPU drivers, conda environments with different versions of python, and pylzy package in all environments.
Can be used as default user environment. User should use it as parent image for their image with custom environment.

**Default user image** split on **[base](DefaultEnv.Base.Dockerfile)** and **[final](DefaultEnv.Dockerfile)** images.
* **Base** image contains all dependencies (GPU drivers, conda, etc.) and updates rarely.
* **Final** image extends **base** image with python packages (including pylzy) and updates with every pylzy update.

(Suggestion: rename to _UserDefault.Dockerfile_)

## Test user image

Light version of **default user image**. Used in tests.
It contains only py39 conda-env, and has no GPU drivers.

**Default user image** also split on **[base](TestEnv.Base.Dockerfile)** and **[final](TestEnv.Dockerfile)** images.

(Suggestion: rename to _UserTest.Dockerfile_)
