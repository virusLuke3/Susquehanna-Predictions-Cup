"""Identity and source evidence shared by isolated strategy versions."""

import hashlib
from typing import Literal

from pydantic import Field, model_validator

from sigbot.domain import AwareTime, StrictModel, content_hash


class EventIdentity(StrictModel):
    year: int = Field(ge=2026, le=2026)
    state: str = Field(min_length=1)
    office: Literal["HOUSE", "SENATE", "GOVERNOR", "CHAMBER_CONTROL"]
    chamber: str = Field(min_length=1)
    district: str = Field(min_length=1)
    boundary_version: str = Field(min_length=1)
    seat: str = Field(min_length=1)
    election_kind: Literal["GENERAL", "PRIMARY", "SPECIAL"]

    def event_key(self, synthetic: bool) -> str:
        return ("SIM-EVENT-" if synthetic else "EVENT-") + content_hash(self.model_dump())[:24]


class Evidence(StrictModel):
    source_url: str = Field(min_length=1)
    kind: Literal["SYNTHETIC", "OFFICIAL", "ORIGINAL_PUBLIC", "UNVERIFIED"]
    text: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    published_at: AwareTime | None = None
    received_at: AwareTime
    available_at: AwareTime

    @model_validator(mode="after")
    def validate_evidence(self):
        if hashlib.sha256(self.text.encode()).hexdigest() != self.sha256:
            raise ValueError("evidence content hash mismatch")
        if self.received_at > self.available_at or (
            self.published_at is not None and self.published_at > self.available_at
        ):
            raise ValueError("evidence availability precedes receipt/publication")
        return self
