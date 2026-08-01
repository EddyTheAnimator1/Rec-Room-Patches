from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


SOURCE_REPOSITORY = "https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words"
DEFAULT_COMMIT = "5faf2ba42d7b1c0977169ec3611df25a3c08eb13"
DEFAULT_LANGUAGES = ("en",)
MINIMUM_FILE_BYTES = 100
MAXIMUM_FILE_BYTES = 2 * 1024 * 1024
MAXIMUM_TERM_CHARACTERS = 512


def _download(commit: str, language: str) -> bytes:
    url = (
        "https://raw.githubusercontent.com/"
        "LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words/"
        f"{commit}/{language}"
    )
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Rec-Room-Patches-filter-updater/1"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        status = int(getattr(response, "status", 200))
        if status != 200:
            raise RuntimeError(f"{language}: upstream returned HTTP {status}.")
        body = response.read(MAXIMUM_FILE_BYTES + 1)
    if not MINIMUM_FILE_BYTES <= len(body) <= MAXIMUM_FILE_BYTES:
        raise RuntimeError(f"{language}: unexpected download size {len(body)} bytes.")
    return body


def _review(body: bytes, language: str) -> list[str]:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{language}: upstream file is not valid UTF-8.") from exc
    if "\x00" in text:
        raise RuntimeError(f"{language}: upstream file contains a NUL byte.")
    terms: list[str] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        term = raw_line.strip()
        if not term or term in seen:
            continue
        if len(term) > MAXIMUM_TERM_CHARACTERS:
            raise RuntimeError(f"{language}: term exceeds {MAXIMUM_TERM_CHARACTERS} characters.")
        seen.add(term)
        terms.append(term)
    if len(terms) < 10:
        raise RuntimeError(f"{language}: reviewed file contains too few non-empty terms.")
    return terms


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="wb",
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _old_terms(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    try:
        return {line for line in path.read_text(encoding="utf-8").splitlines() if line}
    except (OSError, UnicodeError):
        return set()


def update(snapshot_dir: Path, commit: str, languages: tuple[str, ...]) -> None:
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit.casefold()):
        raise RuntimeError("commit must be a full 40-character Git SHA.")
    reviewed: dict[str, bytes] = {}
    metadata_files: dict[str, dict[str, object]] = {}
    summaries: list[str] = []
    for language in languages:
        if not language or Path(language).name != language:
            raise RuntimeError(f"Unsafe language filename: {language!r}")
        body = _download(commit, language)
        terms = _review(body, language)
        encoded = ("\n".join(terms) + "\n").encode("utf-8")
        reviewed[language] = encoded
        old = _old_terms(snapshot_dir / "languages" / language)
        new = set(terms)
        summaries.append(
            f"{language}: {len(terms)} terms, +{len(new - old)}, -{len(old - new)}"
        )
        metadata_files[language] = {
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "bytes": len(encoded),
            "terms": len(terms),
        }

    metadata = {
        "source_repository": SOURCE_REPOSITORY,
        "commit": commit,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "license": "CC-BY-4.0",
        "languages": list(languages),
        "files": metadata_files,
    }
    # Nothing is activated until every selected language has downloaded and
    # passed review. Keep byte-for-byte backups so a local disk/write failure
    # cannot leave new language files paired with old hashes (or vice versa).
    activation = {
        **{
            snapshot_dir / "languages" / language: encoded
            for language, encoded in reviewed.items()
        },
        snapshot_dir / "source.json": (
            json.dumps(metadata, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8"),
    }
    previous: dict[Path, bytes | None] = {}
    for path in activation:
        previous[path] = path.read_bytes() if path.is_file() else None
    try:
        # Metadata is deliberately last: a running server either observes the
        # prior complete snapshot or fails closed during this tiny update
        # window; it never trusts new terms under stale hashes.
        for path, encoded in activation.items():
            _atomic_write(path, encoded)
    except Exception:
        for path, original in previous.items():
            if original is None:
                path.unlink(missing_ok=True)
            else:
                _atomic_write(path, original)
        raise
    print(f"Activated reviewed filter snapshot {commit}:")
    for summary in summaries:
        print(f"  {summary}")


def main() -> None:
    default_snapshot = Path(__file__).resolve().parents[1] / "FILTERS"
    parser = argparse.ArgumentParser(description="Update the reviewed local bad-word snapshot.")
    parser.add_argument("--commit", default=DEFAULT_COMMIT)
    parser.add_argument(
        "--language",
        action="append",
        dest="languages",
        help="Upstream language filename. May be supplied more than once.",
    )
    parser.add_argument("--snapshot-dir", type=Path, default=default_snapshot)
    arguments = parser.parse_args()
    languages = tuple(dict.fromkeys(arguments.languages or DEFAULT_LANGUAGES))
    try:
        update(arguments.snapshot_dir.resolve(), arguments.commit.casefold(), languages)
    except (OSError, RuntimeError, urllib.error.URLError, urllib.error.HTTPError) as exc:
        raise SystemExit(f"Filter update failed; last known-good snapshot was preserved: {exc}") from exc


if __name__ == "__main__":
    main()
