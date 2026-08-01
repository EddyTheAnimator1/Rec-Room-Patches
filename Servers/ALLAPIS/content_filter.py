from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ZERO_WIDTH_CODEPOINTS = {
    "\u200b",
    "\u200c",
    "\u200d",
    "\u2060",
    "\ufeff",
}
PHRASE_SEPARATOR_PATTERN = r"(?:[\s_\-./\\|,]+)"
VALID_POLICIES = {"reject_profile", "censor", "preserve_evidence"}


class ContentFilterError(RuntimeError):
    """Raised when a filtered write cannot be evaluated safely."""


class ProhibitedProfileText(ContentFilterError):
    """Raised when reject_profile finds prohibited text."""


@dataclass(frozen=True)
class FilterResult:
    original_text: str
    output_text: str
    blocked: bool
    changed: bool
    matched_term_ids: tuple[str, ...]
    policy: str
    context: str
    list_version: str


def _normalize_fragment(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = "".join(character for character in normalized if character not in ZERO_WIDTH_CODEPOINTS)
    return re.sub(r"\s+", " ", normalized).strip()


def _normalized_text_with_source_map(value: str) -> tuple[str, list[int]]:
    characters: list[str] = []
    source_indexes: list[int] = []
    for source_index, source_character in enumerate(value):
        if source_character in ZERO_WIDTH_CODEPOINTS:
            continue
        normalized = unicodedata.normalize("NFKC", source_character).casefold()
        for character in normalized:
            if character in ZERO_WIDTH_CODEPOINTS:
                continue
            characters.append(character)
            source_indexes.append(source_index)
    return "".join(characters), source_indexes


def _term_pattern(term: str) -> str:
    words = [word for word in re.split(r"\s+", term) if word]
    # Treat repeated characters as emphasis, not as a filter bypass. Each
    # canonical alphanumeric character consumes one or more copies, so `sex`,
    # `sexx`, `sexxx`, and `sseeexx` are the same prohibited token while word
    # boundaries still prevent matches inside unrelated longer words.
    def word_pattern(word: str) -> str:
        pieces: list[str] = []
        index = 0
        while index < len(word):
            character = word[index]
            run_end = index + 1
            while run_end < len(word) and word[run_end] == character:
                run_end += 1
            minimum = run_end - index
            if character.isalnum():
                pieces.append(f"(?:{re.escape(character)}){{{minimum},}}")
            else:
                pieces.append(re.escape(character) * minimum)
            index = run_end
        return "".join(pieces)

    escaped = PHRASE_SEPARATOR_PATTERN.join(word_pattern(word) for word in words)
    return rf"(?<!\w)(?:{escaped})(?!\w)"


class ContentFilter:
    def __init__(
        self,
        snapshot_dir: Path,
        *,
        enabled: bool = True,
        replacement: str = "#@(!@#",
        allowed_words: Iterable[str] = (),
        max_input_characters: int = 20_000,
        terms: Iterable[str] | None = None,
        list_version: str | None = None,
    ):
        self.snapshot_dir = snapshot_dir
        self.enabled = bool(enabled)
        self.replacement = replacement
        self.max_input_characters = int(max_input_characters)
        self.allowed_words = {
            normalized
            for value in allowed_words
            if (normalized := _normalize_fragment(str(value)))
        }
        loaded_terms, loaded_version = (
            (list(terms), list_version or "injected")
            if terms is not None
            else self._load_snapshot()
        )
        normalized_terms = {
            normalized
            for value in loaded_terms
            if (normalized := _normalize_fragment(str(value)))
            and normalized not in self.allowed_words
        }
        self.terms = tuple(sorted(normalized_terms, key=lambda value: (-len(value), value)))
        self.list_version = str(loaded_version or "unavailable")
        self._pattern = self._compile_pattern(self.terms)

    def _load_snapshot(self) -> tuple[list[str], str]:
        metadata_path = self.snapshot_dir / "source.json"
        language_dir = self.snapshot_dir / "languages"
        if not metadata_path.is_file() or not language_dir.is_dir():
            return [], "unavailable"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeError):
            return [], "unavailable"
        languages = metadata.get("languages")
        if not isinstance(languages, list) or not languages:
            return [], "unavailable"
        terms: list[str] = []
        for language in languages:
            filename = Path(str(language)).name
            if filename != str(language) or not filename:
                return [], "unavailable"
            path = language_dir / filename
            try:
                raw = path.read_text(encoding="utf-8")
            except (OSError, UnicodeError):
                return [], "unavailable"
            expected_hash = (
                metadata.get("files", {}).get(filename, {}).get("sha256")
                if isinstance(metadata.get("files"), dict)
                else None
            )
            actual_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
            if not expected_hash or not isinstance(expected_hash, str) or actual_hash != expected_hash:
                return [], "unavailable"
            terms.extend(line for line in raw.splitlines() if line.strip())
        version = str(metadata.get("commit") or metadata.get("version") or "unknown")
        return terms, version

    @staticmethod
    def _compile_pattern(terms: tuple[str, ...]) -> re.Pattern[str] | None:
        if not terms:
            return None
        alternatives = [_term_pattern(term) for term in terms]
        return re.compile("|".join(alternatives), flags=re.UNICODE)

    @property
    def ready(self) -> bool:
        return self._pattern is not None and bool(self.terms)

    def startup_summary(self) -> str:
        if not self.enabled:
            return "Content filtering is DISABLED by operator configuration."
        if not self.ready:
            return "Content filtering is enabled but no valid reviewed snapshot is available; filtered writes fail closed."
        return (
            f"Content filtering enabled: snapshot={self.list_version}, "
            f"normalized_terms={len(self.terms)}."
        )

    def apply(self, value: str, *, policy: str, context: str) -> FilterResult:
        if policy not in VALID_POLICIES:
            raise ValueError(f"Unknown content-filter policy: {policy}")
        if not isinstance(value, str):
            raise TypeError("Content-filter input must be text.")
        if len(value) > self.max_input_characters:
            raise ValueError("Text exceeds the content-filter input limit.")
        if not self.enabled:
            return FilterResult(
                original_text=value,
                output_text=value,
                blocked=False,
                changed=False,
                matched_term_ids=(),
                policy=policy,
                context=context,
                list_version=self.list_version,
            )
        if not self.ready or self._pattern is None:
            raise ContentFilterError("The reviewed content-filter snapshot is unavailable.")

        normalized, source_map = _normalized_text_with_source_map(value)
        spans: list[tuple[int, int, str]] = []
        for match in self._pattern.finditer(normalized):
            if match.start() >= len(source_map) or match.end() <= 0:
                continue
            source_start = source_map[match.start()]
            source_end = source_map[match.end() - 1] + 1
            matched_normalized = _normalize_fragment(match.group(0))
            term_id = hashlib.sha256(matched_normalized.encode("utf-8")).hexdigest()[:16]
            spans.append((source_start, source_end, term_id))

        if not spans:
            return FilterResult(
                original_text=value,
                output_text=value,
                blocked=False,
                changed=False,
                matched_term_ids=(),
                policy=policy,
                context=context,
                list_version=self.list_version,
            )

        term_ids = tuple(dict.fromkeys(span[2] for span in spans))
        if policy == "reject_profile":
            raise ProhibitedProfileText("Profile text did not pass validation.")

        # Combined alternatives are longest-first, but adjacent matches can
        # still overlap after Unicode source mapping. Merge before replacing so
        # the public output never contains fragments of a matched phrase.
        merged: list[tuple[int, int]] = []
        for start, end, _ in sorted(spans):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        pieces: list[str] = []
        cursor = 0
        for start, end in merged:
            pieces.append(value[cursor:start])
            pieces.append(self.replacement)
            cursor = end
        pieces.append(value[cursor:])
        output = "".join(pieces)
        return FilterResult(
            original_text=value,
            output_text=output,
            blocked=False,
            changed=output != value,
            matched_term_ids=term_ids,
            policy=policy,
            context=context,
            list_version=self.list_version,
        )


def environment_enabled(default: bool) -> bool:
    raw = os.getenv("RECROOM_FILTERS")
    if raw is None:
        return default
    return raw.strip().casefold() not in {"0", "false", "no", "off"}


def environment_allowed_words(default: Iterable[str]) -> set[str]:
    configured = os.getenv("RECROOM_FILTER_ALLOWED_WORDS")
    if configured is None:
        return {str(value) for value in default}
    return {
        value.strip()
        for value in configured.split(",")
        if value.strip()
    }
