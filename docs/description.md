# LZY Services

### IAM
Authentication and authorization service inside the lzy.

Terraform: [here](../deployment/tf/modules/v2/iam.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/iam/1.0/images/sha256-f9b643a35e0902572a11d244a1302932e21fc1d501e9b7e76da575de3240be0f)

### Lzy Service
Lzy entrypoint. All requests from the client come here and been routed to the next services. Has public IP.

Terraform: [here](../deployment/tf/modules/v2/lzy-service.tf здесь)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/lzy-service/1.1/images/sha256-3941b3b56b0d9d536c02925ee4c77696b11db63a3575b7db1a60c0baa94a56ac)

### Graph executor
Graph executor accepts requests for graphs execution. Graph is divided into tasks. After that, execution requests for this tasks are sent to the scheduler.   

Terraform: [here](../deployment/tf/modules/v2/graph-executor.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/graph-executor/1.1/images/sha256-0e081f7f53c867759980f0f2fa0d140108ade6b5d7b668a4de2a540cabb3c33c)

### Scheduler
Scheduler is responsible for scheduling and executions of the tasks. It send requests for VM allocation and task execution.

Terraform: [here](../deployment/tf/modules/v2/scheduler.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/scheduler/1.0/images/sha256-88488497fe11d980b62abf6af40d8b2138e6533e4d69f0e21f221ebca98733d9)

### Allocator
Service for VM allocation. Interacts with k8s clusters. 

Terraform: [here](https://github.com/lambdazy/lzy/tree/81f9fb01d8f29cfd41eae1d7b3587a53f714f1af/deployment/tf/modules/v2/allocator.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/allocator/1.1/images/sha256-8785f078a2dab8d743850cb98d049dab2fa06f6fa758252f16994180df43e7fb)

### Worker
Service which runs on the allocated VM. Responsible for execution of particular task.

Terraform: [here](../deployment/tf/modules/v2/worker-nodegroup.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/worker/1.2/images/sha256-314ab64ce0669f093a41641d8986ad55049b744368f9f64b823bc611c214c053)

### Portal
Service which runs on the allocated VM. Responsible for connection between workers and outer world, e.g. interaction with s3 for saving data on whiteboard.

Terraform: [here](../deployment/tf/modules/v2/portals-nodegroup.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/portal/1.1/images/sha256-d63db3968bfdf9af00e16d995c16f1cd224733135275fa9527510a90d20510f6)

### Channel-manager
Service that establish data connection between workers themselves and between workers and portal.

Terraform: [here](../deployment/tf/modules/v2/channel-manager.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/channel-manager/1.0/images/sha256-b2676258659e163d77015554fce5e27b8807364cf1ce9dc12dccf8dede4487d2)

### Storage
Service for storing inner S3 accounts.

Terraform: [here](../deployment/tf/modules/v2/storage.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/storage/1.0/images/sha256-e9b452d68d98a6f86b2da47324c496a8cda14cf568ca589865636a866e973d15)

### Whiteboard Service
Service which stores whiteboards with their description and allows select them by tags, time, etc.

Terraform: [here](../deployment/tf/modules/v2/whiteboard.tf)
<br>
Latest image: [here](https://hub.docker.com/layers/lzydock/whiteboard/1.0/images/sha256-7ad1bd2b044ad3cfdcfe6ff51b2628de83c7527762bac125f3f08f4d44a66bab)

### Site

Old name - backoffice, may appear in some terraform scripts.

Terraform: [here](../deployment/tf/modules/v2/site.tf)
<br>
Frontend image: [here](https://hub.docker.com/layers/lzydock/site-frontend/1.0/images/sha256-5ae6483caecec2c0866b79acccef79273556bf1813b1d39959e29099f5a98645)
<br>
Backend image: [here](https://hub.docker.com/layers/lzydock/site/1.1/images/sha256-f673a65bc35dc3032e2f679586afc7e58cc08229802da1ae3289369c5622c3b4)

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