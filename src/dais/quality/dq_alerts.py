"""Pluggable DQ/control-gate alert channels. Selected by name
(`quality.quarantine.alert.channel`) so a new channel is a new class, not
a change to the callers.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import structlog

log = structlog.get_logger(__name__)


class Alerter(ABC):
    @abstractmethod
    def send(self, destination: str, message: str, context: dict[str, Any]) -> None: ...


class LogAlerter(Alerter):
    def send(self, destination: str, message: str, context: dict[str, Any]) -> None:
        log.warning("dq_alert", destination=destination, message=message, **context)


class WebhookAlerter(Alerter):
    def send(self, destination: str, message: str, context: dict[str, Any]) -> None:
        import requests

        try:
            response = requests.post(destination, json={"message": message, **context}, timeout=10)
            response.raise_for_status()
        except requests.RequestException:
            log.error("dq_alert_webhook_failed", destination=destination, message=message)
            raise


class EmailAlerter(Alerter):
    def send(self, destination: str, message: str, context: dict[str, Any]) -> None:
        raise NotImplementedError(
            "EmailAlerter is not wired to an SMTP/SES backend yet - "
            "use channel: log or channel: webhook for now"
        )


_REGISTRY: dict[str, type[Alerter]] = {
    "log": LogAlerter,
    "webhook": WebhookAlerter,
    "email": EmailAlerter,
}


def get_alerter(channel: str) -> Alerter:
    try:
        return _REGISTRY[channel]()
    except KeyError as exc:
        available = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"no alerter registered for channel {channel!r}; available: {available}") from exc
