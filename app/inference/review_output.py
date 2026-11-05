"""Pure review normalization and validation; no inference or logging."""

import re

from app.errors import AppError, ValidationReason
from app.inference.prompts import SECTION_TITLES

REQUIRED_SECTIONS = ("summary", "findings", "suggestions")
INVALID_REVIEW_MESSAGE = (
    "The model returned an unusable review. Try a smaller, self-contained snippet."
)
COMMON_IDENTIFIERS = {
    "async",
    "await",
    "bool",
    "boolean",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "default",
    "def",
    "dict",
    "double",
    "elif",
    "else",
    "except",
    "false",
    "finally",
    "float",
    "for",
    "from",
    "function",
    "import",
    "int",
    "lambda",
    "list",
    "none",
    "null",
    "pass",
    "print",
    "private",
    "protected",
    "public",
    "range",
    "return",
    "self",
    "set",
    "static",
    "str",
    "string",
    "switch",
    "this",
    "throw",
    "throws",
    "true",
    "void",
    "while",
    "with",
    "yield",
}


def normalize_section_body(section: str, result: str) -> str:
    """Remove one echoed current heading and reject reserved headings in the body."""

    title = SECTION_TITLES[section]
    body = result.strip()
    body = re.sub(
        rf"\A#{{1,6}}\s+{re.escape(title)}\s*#*\s*(?:\r?\n|\Z)",
        "",
        body,
        count=1,
        flags=re.IGNORECASE,
    ).strip()
    if re.search(
        r"(?im)^[ \t]{0,3}#{1,6}\s+(summary|findings|suggestions)\s*#*\s*$",
        body,
    ):
        raise AppError(
            "invalid_model_response",
            INVALID_REVIEW_MESSAGE,
            502,
            validation_reason=ValidationReason.UNEXPECTED_SECTION_HEADING,
        )
    if not body:
        raise AppError(
            "invalid_model_response",
            INVALID_REVIEW_MESSAGE,
            502,
            validation_reason=ValidationReason.EMPTY_SECTION,
        )
    return body


def trim_capped_section_tail(body: str, section_limit_reached: bool) -> tuple[str, bool]:
    """Drop only an unfinished suffix from a section that exhausted its token budget."""

    if not section_limit_reached:
        return body, False
    closing_characters = r"`'\"”’\)\]\}\*_"
    complete_ending = rf"[.!?][{closing_characters}]*\Z"
    if re.search(complete_ending, body):
        return body, False
    boundaries = list(re.finditer(rf"[.!?][{closing_characters}]*(?=\s|\Z)", body))
    if boundaries:
        trimmed = body[: boundaries[-1].end()].rstrip()
        if trimmed:
            return trimmed, True
    raise AppError(
        "invalid_model_response",
        INVALID_REVIEW_MESSAGE,
        502,
        validation_reason=ValidationReason.TRUNCATED_SECTION,
    )


def validate_review_output(result: str, source: str) -> str:
    """Reject obviously malformed or detached generations without logging their contents."""

    headings = list(
        re.finditer(
            r"(?im)^#{1,3}\s+(summary|findings|suggestions)\s*#*\s*$",
            result,
        )
    )
    first = {}
    for match in headings:
        first.setdefault(match.group(1).casefold(), match)
    if any(section not in first for section in REQUIRED_SECTIONS):
        raise AppError(
            "invalid_model_response",
            INVALID_REVIEW_MESSAGE,
            502,
            validation_reason=ValidationReason.MISSING_SECTIONS,
        )
    ordered = [first[section] for section in REQUIRED_SECTIONS]
    if [match.start() for match in ordered] != sorted(match.start() for match in ordered):
        raise AppError(
            "invalid_model_response",
            INVALID_REVIEW_MESSAGE,
            502,
            validation_reason=ValidationReason.SECTION_ORDER,
        )
    for index, match in enumerate(ordered):
        end = ordered[index + 1].start() if index + 1 < len(ordered) else len(result)
        if not result[match.end() : end].strip():
            raise AppError(
                "invalid_model_response",
                INVALID_REVIEW_MESSAGE,
                502,
                validation_reason=ValidationReason.EMPTY_SECTION,
            )

    source_identifiers = {
        token.casefold()
        for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{2,}\b", source)
        if token.casefold() not in COMMON_IDENTIFIERS
    }
    result_identifiers = {
        token.casefold() for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{2,}\b", result)
    }
    if source_identifiers and source_identifiers.isdisjoint(result_identifiers):
        raise AppError(
            "invalid_model_response",
            INVALID_REVIEW_MESSAGE,
            502,
            validation_reason=ValidationReason.DETACHED_SOURCE,
        )
    return result
