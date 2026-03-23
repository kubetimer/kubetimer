import argparse
import time
from kubernetes import client, config

DEPLOYMENTS_COUNT = 2000
BATCH_SIZE = 200  # Create in batches to be nice to Minikube


def create_zombies(*, with_pods: bool = False, count: int = DEPLOYMENTS_COUNT):
    config.load_kube_config()
    api = client.AppsV1Api()

    mode = "with-pods (replicas=1)" if with_pods else "zero-replica"
    print(f"Creating {count} {mode} zombie deployments...")

    container = {"name": "pause", "image": "registry.k8s.io/pause:3.10"}
    if with_pods:
        container["resources"] = {
            "requests": {"cpu": "1m", "memory": "4Mi"},
            "limits":   {"cpu": "2m", "memory": "8Mi"},
        }

    base_deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "labels": {"app": "kubetimer-zombie"},
            "name": "zombie-0",
            "annotations": {"kubetimer.io/ttl": "2025-01-01T00:00:00Z"},
        },
        "spec": {
            "replicas": 1 if with_pods else 0,
            "selector": {"matchLabels": {"app": "zombie"}},
            "template": {
                "metadata": {"labels": {"app": "zombie"}},
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [container],
                },
            },
        },
    }

    start = time.time()
    for i in range(count):
        name = f"zombie-{i}"
        base_deployment["metadata"]["name"] = name
        try:
            api.create_namespaced_deployment(namespace="default", body=base_deployment)
        except client.exceptions.ApiException as e:
            if e.status != 409:  # Ignore "Already Exists"
                print(f"Error: {e}")

        if i % BATCH_SIZE == 0:
            print(f"Created {i}/{count}...")

    print(
        f"Setup Complete. Created {count} "
        f"deployments in {time.time()-start:.2f}s"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk-create expired zombie deployments")
    parser.add_argument(
        "--with-pods", action="store_true", default=False,
        help="Create with 1 replica (actual pods) instead of 0",
    )
    parser.add_argument(
        "--count", type=int, default=DEPLOYMENTS_COUNT,
        help=f"Number of zombies to create (default: {DEPLOYMENTS_COUNT})",
    )
    args = parser.parse_args()
    create_zombies(with_pods=args.with_pods, count=args.count)
