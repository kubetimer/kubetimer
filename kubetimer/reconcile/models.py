from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TtlDeployment:
    name: str
    namespace: str
    uid: str
    ttl_value: datetime
