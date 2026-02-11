import time
from kubernetes import client, config

def watch_race():
    config.load_kube_config()
    api = client.AppsV1Api()
    
    print("Waiting for deployments to exist...")
    while True:
        print("Checking for zombies...")
        count = len(api.list_namespaced_deployment("default", label_selector="app=zombie").items)
        print(f"Found {count} zombies.")
        if count > 0:
            print(f"Detected {count} zombies. STARTING TIMER.")
            break
        # time.sleep(1)

    start_time = time.time()
    
    while True:
        count = len(api.list_namespaced_deployment("default", label_selector="app=zombie").items)
        if count == 0:
            end_time = time.time()
            break
        print(f"Remaining: {count}")
        # time.sleep(1)
        
    duration = end_time - start_time
    print(f"\n🏁 RACE FINISHED!")
    print(f"Time Taken: {duration:.2f} seconds")

if __name__ == "__main__":
    watch_race()