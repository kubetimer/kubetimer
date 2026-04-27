from typing import Literal, Mapping

ActionLiteral = Literal["hibernate", "delete"]

ACTION_HIBERNATE: ActionLiteral = "hibernate"
ACTION_DELETE: ActionLiteral = "delete"

VALID_ACTIONS: frozenset[str] = frozenset({ACTION_HIBERNATE, ACTION_DELETE})

ACTION_ANNOTATION_SUFFIX: str = "action"
ORIGINAL_REPLICAS_SUFFIX: str = "original-replicas"


def resolve_action(
    annotations: Mapping[str, str] | None,
    action_key: str,
    default: ActionLiteral,
) -> ActionLiteral:
    """Resolve the lifecycle action from a resource's annotations.

    Returns ``default`` when the annotation is absent. Raises ``ValueError``
    when the annotation is set but not a recognized action.
    """
    if not annotations:
        return default

    raw = annotations.get(action_key)
    if raw is None:
        return default

    value = raw.strip().lower()
    if value not in VALID_ACTIONS:
        raise ValueError(f"invalid_action: {raw!r}")

    return value  # type: ignore[return-value]
