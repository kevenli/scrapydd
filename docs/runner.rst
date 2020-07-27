Runner
======

Runner is the unit which controls executing `scrapy` command on specify
scrapy project package in system, i.e. extract spider list or executing
crawling job.

There two types of built-in runners in the system. a `VenvRunner` and
a `DockerRunner`

VenvRunner
----------
To isolate environment from each spider project execution. System will
create a temporary environment for each command running, with isolated
executables, libraries, and files.

VenvRunner will create a virtualenv environment, run command in a
sub process and clean it up when command finished.

It is the default runner in system, which do not need any additional
operation to let it to be enabled.


DockerRunner
------------
Docker runner provides a more start-fast and more secure mechanism to
run a job, it start the sub-process in a container, in which the host
will not be threatened by any third-party spider project.

And it need additional requisition to be enabled.

* A docker daemon.
* Set `runner_type` to `docker` in config file.
* Optional: set `runner_docker_image` to whatever image name you what
   that the runner can call pancli command on. It is `pansihub/pancli`
   which is built by the author by default.
* Pull the image before you use. The system will not pull any docker image
   at the runtime. `docker pull pansihub/pancli`

If you want run server/agent in docker, it is possible use the DockerRunner.
To let a server/agent process access the docker daemon outside the container,
You can map the host's docker sock file into container.

In docker-compose it can be setted:

   volumes:
     - "/var/run/docker.sock:/var/run/docker.sock"

To run in docker command it can be

    docker run -v "/var/run/docker.sock:/var/run/docker.sock" ...


