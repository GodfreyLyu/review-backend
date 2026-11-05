"""Review generation policy and measurements from completed remote generations."""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field

from app.errors import AppError
from app.inference.prompts import SECTION_SPECS


@dataclass
class SectionMetrics:
    generated_tokens: int
    input_tokens: int
    limit_reached: bool = False
    trailing_fragment_removed: bool = False
    # Timing fields are optional in the RPC contract, even for completed generations.
    generation_ms: int | None = None
    first_token_ms: int | None = None


@dataclass
class GenerationMetrics:
    output_token_limit: int
    sections: dict[str, SectionMetrics] = field(default_factory=dict)

    def fields(self) -> dict[str, object]:
        generated_tokens = sum(section.generated_tokens for section in self.sections.values())
        fields: dict[str, object] = {
            "duration_ms": max(
                0, sum(section.generation_ms or 0 for section in self.sections.values())
            ),
            "generated_tokens": generated_tokens,
            "output_limit_reached": generated_tokens >= self.output_token_limit,
            "output_token_limit": self.output_token_limit,
        }
        for key, attribute in (
            ("section_generated_tokens", "generated_tokens"),
            ("section_generation_ms", "generation_ms"),
            ("section_first_token_ms", "first_token_ms"),
            ("section_input_tokens", "input_tokens"),
            ("section_limits_reached", "limit_reached"),
            ("section_trailing_fragments_removed", "trailing_fragment_removed"),
        ):
            fields[key] = {
                name: getattr(metrics, attribute) for name, metrics in self.sections.items()
            }
        return fields


def check_deadline(stop: threading.Event, deadline: float) -> None:
    if stop.is_set() or time.monotonic() >= deadline:
        raise AppError("inference_timeout", "Review timed out. Try a shorter submission.", 504)


SECTION_TOKEN_WEIGHTS = {"summary": 9, "findings": 22, "suggestions": 17}


def derive_generation_seed(model_revision: str, language: str, section: str, source: str) -> int:
    """Derive a stable nonnegative 63-bit seed without retaining source-derived data."""

    digest = hashlib.sha256(b"local-llm-code-review-generation-seed-v1")
    for component in (model_revision, language, section, source):
        encoded = component.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return int.from_bytes(digest.digest()[:8], "big") & ((1 << 63) - 1)


def allocate_section_token_limits(total_limit: int) -> dict[str, int]:
    """Split one review-wide output budget across the three fixed sections."""

    if total_limit < len(SECTION_SPECS):
        raise ValueError("The review output token limit must allow at least one token per section.")
    sections = [section for section, _, _ in SECTION_SPECS]
    # Reserve one token per section, then divide the rest by the fixed weights.
    remaining = total_limit - len(sections)
    total_weight = sum(SECTION_TOKEN_WEIGHTS.values())
    limits = {}
    remainders = []
    for position, section in enumerate(sections):
        quotient, remainder = divmod(remaining * SECTION_TOKEN_WEIGHTS[section], total_weight)
        limits[section] = 1 + quotient
        remainders.append((remainder, -position, section))
    # Give leftover tokens to the largest fractional shares; ties follow section order.
    leftover = total_limit - sum(limits.values())
    for _, _, section in sorted(remainders, reverse=True)[:leftover]:
        limits[section] += 1
    return limits
