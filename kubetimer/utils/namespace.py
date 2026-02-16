from typing import List


def should_scan_namespace(
    namespace: str,
    include_namespaces: List[str],
    exclude_namespaces: List[str],
) -> bool:
    if namespace in exclude_namespaces:
        return False
    if not include_namespaces:
        return True
    return namespace in include_namespaces