KUBECTL_DEV = kubectl
HELM_DEV = helm

# NFS storage (override for non-default clusters/nodes)
NFS_NAMESPACE ?= nfs-system
NFS_STORAGE_CLASS ?= nfs
NFS_EXPORT_PATH ?= /srv/nfs/k8s
NFS_NODE_HOSTNAME ?=
NFS_SERVER ?=
NFS_PROVISIONER_NAME ?= cluster.local/nfs-subdir-external-provisioner
NFS_DIR = $(CURDIR)/nfs

# Configuration
NAMESPACE = sdg-workflow
RELEASE_NAME = sdg-workflow-controller
CHART_PATH = deploy
VALUES_DEV = deploy/values.yaml
SECRET_VALUES_DEV = deploy/secret_values_dev.yaml
SECRETS_ENV = secrets.env
# Replace with an NGC org/team path that NGC_API_KEY in secrets.env can push.
AIRFLOW_IMAGE_REPOSITORY ?= nvcr.io/<your-org>/<your-team>/sdg-workflow-airflow
AIRFLOW_IMAGE_TAG ?= latest
PYTHON_REQUIRED_MAJOR = 3
PYTHON_REQUIRED_MINOR = 11
