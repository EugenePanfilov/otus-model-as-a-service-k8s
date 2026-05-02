#cloud-config

write_files:
  - path: /home/${instance_user}/mlflow.conf
    owner: ${instance_user}:${instance_user}
    permissions: "0600"
    encoding: b64
    defer: true
    content: ${mlflow_conf}

  - path: /home/${instance_user}/mlflow.service
    owner: ${instance_user}:${instance_user}
    permissions: "0644"
    encoding: b64
    defer: true
    content: ${mlflow_service}

  - path: /home/${instance_user}/setup_mlflow.sh
    owner: ${instance_user}:${instance_user}
    permissions: "0755"
    encoding: b64
    defer: true
    content: ${setup_mlflow}

runcmd:
  - [ bash, /home/${instance_user}/setup_mlflow.sh ]
  