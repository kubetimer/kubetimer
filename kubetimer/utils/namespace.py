from collections.abc import Container


def should_scan_namespace(
    namespace: str,
    include_namespaces: Container[str],
    exclude_namespaces: Container[str],
) -> bool:
    """Check whether a namespace should be scanned.

    Accepts any container type (list, set, frozenset) for both
    include and exclude filters.  frozenset gives O(1) lookups.
    """
    if namespace in exclude_namespaces:
        return False
    if not include_namespaces:
        return True
    return namespace in include_namespaces
