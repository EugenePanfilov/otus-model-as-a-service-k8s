resource "yandex_compute_instance" "mlflow_server" {
  name               = var.instance_name
  service_account_id = var.service_account_id

  scheduling_policy {
    preemptible = true
  }

  resources {
    cores         = 2
    memory        = 8
    core_fraction = 20
  }

  boot_disk {
    initialize_params {
      image_id = var.ubuntu_image_id
      size     = 30
    }
  }

  network_interface {
    subnet_id = var.subnet_id
    nat       = false
  }

  metadata = {
    ssh-keys           = "${var.instance_user}:${file(var.public_key_path)}"
    serial-port-enable = "1"

    user-data = templatefile("${path.module}/scripts/cloud-init.yaml.tpl", {
      instance_user = var.instance_user
      mlflow_conf = base64encode(templatefile("${path.module}/scripts/mlflow.conf.tpl", {
        s3_endpoint_url   = var.s3_endpoint_url
        s3_bucket_name    = var.s3_bucket_name
        s3_access_key     = var.s3_access_key
        s3_secret_key     = var.s3_secret_key
        mlflow_port       = var.mlflow_port
        postgres_host     = var.postgres_host
        postgres_port     = var.postgres_port
        postgres_db       = var.postgres_db
        postgres_user     = var.postgres_user
        postgres_password = var.postgres_password
      }))
      mlflow_service = base64encode(file("${path.module}/scripts/mlflow.service"))
      setup_mlflow   = base64encode(file("${path.module}/scripts/setup_mlflow.sh"))
    })
  }
}

resource "null_resource" "update_env_mlflow" {
  triggers = {
    mlflow_server_ip = yandex_compute_instance.mlflow_server.network_interface[0].ip_address
  }

  provisioner "local-exec" {
    command = <<EOT
      MLFLOW_TRACKING_URI=http://${yandex_compute_instance.mlflow_server.network_interface[0].ip_address}:${var.mlflow_port}

      if grep -q "^MLFLOW_TRACKING_URI=" ../../.env; then
        sed -i "s|^MLFLOW_TRACKING_URI=.*|MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI|" ../../.env
      else
        echo "MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI" >> ../../.env
      fi
    EOT
  }

  depends_on = [
    yandex_compute_instance.mlflow_server
  ]
}
