import time
from kubernetes import client, config

DEPLOYMENTS_COUNT = 10
BATCH_SIZE = 2 # Create in batches to be nice to Minikube

def create_zombies():
    config.load_kube_config()
    api = client.AppsV1Api()
    
    print(f"Creating {DEPLOYMENTS_COUNT} zombie deployments...")

    base_deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "labels": {"app": "zombie"},
            "name": "zombie-0",
            "annotations": {
                "kubetimer.io/ttl": "2025-01-01T00:00:00Z" 
            }
        },
        "spec": {
            "replicas": 0, # 0 Replicas so we don't kill the CPU with Nginx pods
            "selector": {"matchLabels": {"app": "zombie"}},
            "template": {
                "metadata": {"labels": {"app": "zombie"}},
                "spec": {"containers": [{"name": "pause", "image": "k8s.gcr.io/pause"}]}
            }
        }
    }

    start = time.time()
    for i in range(DEPLOYMENTS_COUNT):
        name = f"zombie-{i}"
        base_deployment["metadata"]["name"] = name
        try:
            api.create_namespaced_deployment(namespace="default", body=base_deployment)
        except client.exceptions.ApiException as e:
            if e.status != 409: # Ignore "Already Exists"
                print(f"Error: {e}")
        
        if i % BATCH_SIZE == 0:
            print(f"Created {i}/{DEPLOYMENTS_COUNT}...")
            
    print(f"✅ Setup Complete. Created {DEPLOYMENTS_COUNT} deployments in {time.time()-start:.2f}s")

if __name__ == "__main__":
    create_zombies()