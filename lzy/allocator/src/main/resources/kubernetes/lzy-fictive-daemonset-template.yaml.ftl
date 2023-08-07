apiVersion: apps/v1
kind: DaemonSet

metadata:
  name: worker-fictive-${pool.name}
  namespace: fictive

spec:
  selector:
    matchLabels:
      name: worker-image-caching

  template:
    metadata:
      labels:
        name: worker-image-caching

    spec:
      initContainers:

<#list workers as worker>
        - name: download-worker-image-${worker.name}
          image: ${worker.image}
          imagePullPolicy: Always
          command: ["sh", "-c", "exit 0"]
</#list>

<#list imgs as img>
        - name: download-dind-images-${img.name}
          image: ${img.dind_image}
          imagePullPolicy: Always
          command: ["sh", "-c", "/entrypoint.sh ${img.additional_images?join(" ")}"]
          securityContext:
            runAsUser: 0
            privileged: true
          volumeMounts:
            - name: host-shared-docker-data-root
              mountPath: /host_shared/docker
              mountPropagation: Bidirectional
</#list>

      containers:
        - name: node-allocator-sync
          image: ${allocator_sync_image}
          imagePullPolicy: Always
          command: ["sh", "-c", "rm /host_shared/docker/started.flag; /entrypoint.sh ${allocator_ip}; tail -f /dev/null"]
          env:
            - name: CLUSTER_ID
              value: ${cluster_id}
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: ALLOCATOR_IP
              value: ${allocator_ip}
          securityContext:
            runAsUser: 0
            privileged: true
          volumeMounts:
            - name: host-shared-docker-data-root
              mountPath: /host_shared/docker
              mountPropagation: Bidirectional

      volumes:
        - name: host-shared-docker-data-root
          hostPath:
            type: DirectoryOrCreate
            path: /host_shared/docker

      hostNetwork: true

      dnsPolicy: ClusterFirstWithHostNet

      nodeSelector:
        lzy.ai/node-pool-label: ${pool.name}
        lzy.ai/node-pool-kind: ${pool.kind}

      restartPolicy: Always

  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: "50%"
