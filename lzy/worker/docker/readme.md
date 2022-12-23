# Docker images description

## Worker image

System image, contains worker application.

Lzy supports images with a custom user environment. Worker implements
[docker-in-docker](https://www.docker.com/blog/docker-can-now-run-within-docker/) scheme here:

* There is an installed [Docker](https://docs.docker.com/get-started/overview/) inside **worker image**
* When a worker container is created, it runs Docker daemon,
which will listen for Docker API requests and manage Docker objects.
* When the worker going to execute a task, it runs an inner Docker container
with custom environment from the user image. The task will be executed inside the inner container.

Worker has an alternative for docker-in-docker scheme.
If the user has no custom environment, the worker can execute tasks in its own environment in outer container.
As a result, **worker image** contains conda environment with pylzy python package.

**Worker image** split on **[base](Worker.Base.Dockerfile)** and **[final](Worker.Dockerfile)** images.
* **Base** image contains all dependencies (GPU drivers, java, docker, conda, etc.) and is rarely updated.
* **Final** image extends **base** image with worker application and python packages (including pylzy). 
It's updated with every worker or pylzy update.

## Default user image

Contains GPU drivers, conda environments with different versions of python, and pylzy package in all environments.
Can be used as default user environment. User should use it as parent image for their image with custom environment.

**Default user image** split on **[base](UserDefault.Base.Dockerfile)** and **[final](UserDefault.Dockerfile)** images.
* **Base** image contains all dependencies (GPU drivers, conda, etc.) and is rarely updated.
* **Final** image extends **base** image with python packages (including pylzy) and is updated with every pylzy update.

## Test user image

Light version of **default user image**. Used in tests.
It contains only py39 conda-env, and has no GPU drivers.

**Default user image** also split on **[base](UserTest.Base.Dockerfile)** and **[final](UserTest.Dockerfile)** images.
