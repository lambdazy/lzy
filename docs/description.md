# LZY Services

### IAM
Authentication and authorization service inside the lzy.

Terraform: [here](../deployment/tf/modules/v2/iam.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/iam/tags)

### Lzy Service
Lzy entrypoint. All requests from the client come here and been routed to the next services. Has public IP.

Terraform: [here](../deployment/tf/modules/v2/lzy-service.tf здесь)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/lzy-service/tags)

### Graph executor
Graph executor accepts requests for graphs execution. Graph is divided into tasks. After that, execution requests for this tasks are sent to the scheduler.   

Terraform: [here](../deployment/tf/modules/v2/graph-executor.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/graph-executor/tags)

### Scheduler
Scheduler is responsible for scheduling and executions of the tasks. It send requests for VM allocation and task execution.

Terraform: [here](../deployment/tf/modules/v2/scheduler.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/scheduler/tags)

### Allocator
Service for VM allocation. Interacts with k8s clusters. 

Terraform: [here](https://github.com/lambdazy/lzy/tree/81f9fb01d8f29cfd41eae1d7b3587a53f714f1af/deployment/tf/modules/v2/allocator.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/allocator/tags)

### Worker
Service which runs on the allocated VM. Responsible for execution of particular task.

Terraform: [here](../deployment/tf/modules/v2/worker-nodegroup.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/worker/tags)

### Portal
Service which runs on the allocated VM. Responsible for connection between workers and outer world, e.g. interaction with s3 for saving data on whiteboard.

Terraform: [here](../deployment/tf/modules/v2/portals-nodegroup.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/portal/tags)

### Channel-manager
Service that establish data connection between workers themselves and between workers and portal.

Terraform: [here](../deployment/tf/modules/v2/channel-manager.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/channel-manager/tags)

### Storage
Service for storing inner S3 accounts.

Terraform: [here](../deployment/tf/modules/v2/storage.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/storage/tags)

### Whiteboard Service
Service which stores whiteboards with their description and allows select them by tags, time, etc.

Terraform: [here](../deployment/tf/modules/v2/whiteboard.tf)
<br>
Latest image: [here](https://hub.docker.com/r/lzydock/whiteboard/tags)

### Site

Old name - backoffice, may appear in some terraform scripts.

Terraform: [here](../deployment/tf/modules/v2/site.tf)
<br>
Frontend image: [here](https://hub.docker.com/layers/lzydock/site-frontend/1.0/images/sha256-5ae6483caecec2c0866b79acccef79273556bf1813b1d39959e29099f5a98645)
<br>
Backend image: [here](https://hub.docker.com/r/lzydock/site/tags)

# Installation structure

### Clusters
- Worker cluster, in which tasks are executed and VM are allocated.
- System cluster with inner lzy service. Portals are allocated here.

### Node-groups:

- Service group. Contains lzy services.
<br>
  Terraform: [here](../deployment/tf/modules/v2/cluster.tf)
- Portal group.
<br>
  Terraform: [here](../deployment/tf/modules/v2/portals-nodegroup.tf)
- Group in worker cluster, where workers are allocated.
<br>
  Terraform: [here](../deployment/tf/modules/v2/worker-nodegroup.tf)
