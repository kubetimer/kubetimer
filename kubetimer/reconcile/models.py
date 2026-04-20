from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TtlDeployment:
    name: str
    namespace: str
    uid: str
    ttl_value: str
    creation_timestamp: datetime | str
    expires_at: str | None = None
