"""Built-in HITL adapters."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from mercury.types import HitlDecision


class NoneHitlConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoneHitl:
    def parse_config(self, raw):
        return NoneHitlConfig.model_validate(raw or {})

    def subscribed_events(self, config):
        del config
        return frozenset()

    async def maybe_pause(self, event, state_view, config):
        del event, state_view, config
        return HitlDecision(pause=False)


class CliGateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pause_on: list[str] = Field(default_factory=list)
    auto_approve: bool = False


class CliGateHitl:
    def parse_config(self, raw):
        return CliGateConfig.model_validate(raw or {})

    def subscribed_events(self, config):
        if config.auto_approve:
            return frozenset()
        return frozenset(config.pause_on)

    async def maybe_pause(self, event, state_view, config):
        del state_view
        if config.auto_approve:
            return HitlDecision(pause=False)
        if event.event_type in config.pause_on:
            return HitlDecision(
                pause=True,
                metadata={
                    "event_type": event.event_type,
                    "tick": event.tick,
                },
            )
        return HitlDecision(pause=False)
