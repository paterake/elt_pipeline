from __future__ import annotations

from typing import Protocol, runtime_checkable

from ._models import SecretValue


@runtime_checkable
class SecretsProvider(Protocol):
    provider_type: str

    def resolve(self, *, path: str) -> SecretValue: ...
