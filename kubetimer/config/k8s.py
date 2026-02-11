from kubernetes import client, config

from kubetimer.utils.logs import setup_logging
logger = setup_logging()


class KubeTimerConfig():
    def __init__(
        self,
        name: str,
        enabled_resources: list[str],
        annotation_key: str,
        dry_run: bool,
        timezone: str,
        namespaces: dict[str, list[str]],
    ):
        self.name = name
        self.enabled_resources = enabled_resources
        self.annotation_key = annotation_key
        self.dry_run = dry_run
        self.timezone = timezone
        self.namespaces = namespaces
        

def load_k8s_config():
    try:
        config.load_incluster_config()
        logger.info("loaded_incluster_config")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("loaded_local_kube_config")


def apps_v1_client() -> client.AppsV1Api:
    return client.AppsV1Api()


def custom_objects_client() -> client.CustomObjectsApi:
    return client.CustomObjectsApi()


def get_kubetimerconfig() -> KubeTimerConfig:
    api = custom_objects_client()
    try:
        config = api.list_cluster_custom_object(
            group="kubetimer.io",
            version="v1",
            plural="kubetimerconfigs",
            limit=1,
        ).get('items')[0]

        kubetimerconfig = KubeTimerConfig(
            name=config['metadata']['name'],
            enabled_resources=config['spec'].get('enabledResources'),
            annotation_key=config['spec'].get('annotationKey', 'kubetimer.io/ttl'),
            dry_run=config['spec'].get('dryRun', False),
            timezone=config['spec'].get('timezone', 'UTC'),
            namespaces=config['spec'].get('namespaces', {}),
        )
        return kubetimerconfig
    
    except Exception as e:
        logger.error("failed_to_get_kubetimerconfig", error=str(e))
        raise
