from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TtlDeployment:
    name: str
    namespace: str
    uid: str
    ttl_value: str
    creation_timestamp: str
    expires_at: str | None = None
