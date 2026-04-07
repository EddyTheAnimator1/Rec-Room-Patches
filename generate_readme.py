import argparse
import csv
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

START_MARKER = "<!-- AUTO_TABLE_START -->"
END_MARKER = "<!-- AUTO_TABLE_END -->"
NOTE_2020 = "I won't be patching these builds or newer ones. . . They are still there to download though, if you'd like."


def extract_manifest_id(raw: str) -> str:
    match = re.search(r"\d+", raw or "")
    if not match:
        raise ValueError(f"Could not find a manifest ID in: {raw!r}")
    return match.group(0)


def parse_seen_date(raw: str) -> datetime:
    raw = (raw or "").strip()
    for fmt in ("%d %B %Y – %H:%M:%S UTC", "%d %B %Y - %H:%M:%S UTC"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported Seen Date format: {raw!r}")


def load_rows(csv_path: Path):
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            seen_raw = (row.get("Seen Date") or "").strip()
            if not seen_raw:
                continue
            dt = parse_seen_date(seen_raw)
            manifest_id = extract_manifest_id(row.get("ManifestID") or "")
            date_only = seen_raw.split(" – ", 1)[0].split(" - ", 1)[0].strip()
            rows.append({
                "dt": dt,
                "year": dt.year,
                "date_only": date_only,
                "manifest_id": manifest_id,
            })
    rows.sort(key=lambda item: (item["dt"].year, item["dt"]))
    return rows


def is_patched(manifest_root: Path, manifest_id: str, patch_filename: str) -> bool:
    return (manifest_root / manifest_id / patch_filename).is_file()


def build_table_markdown(rows, manifest_root: Path, patch_filename: str) -> str:
    by_year = defaultdict(list)
    for row in rows:
        by_year[row["year"]].append(row)

    chunks = [START_MARKER, ""]
    for year in sorted(by_year):
        chunks.append(f"# {year}")
        chunks.append("")
        if year >= 2020:
            chunks.append(NOTE_2020)
            chunks.append("")

        chunks.append("| Date | Manifest | Patched? |")
        chunks.append("| --- | --- | --- |")

        for row in by_year[year]:
            patched = "✅" if is_patched(manifest_root, row["manifest_id"], patch_filename) else "❌"
            chunks.append(f"| {row['date_only']} | `{row['manifest_id']}` | {patched} |")

        chunks.append("")

    chunks.append(END_MARKER)
    chunks.append("")
    return "\n".join(chunks)


def write_output(output_path: Path, generated_block: str):
    if output_path.exists():
        existing = output_path.read_text(encoding="utf-8")
        if START_MARKER in existing and END_MARKER in existing:
            pattern = re.compile(
                re.escape(START_MARKER) + r".*?" + re.escape(END_MARKER),
                flags=re.DOTALL,
            )
            updated = pattern.sub(generated_block.strip(), existing, count=1)
            output_path.write_text(updated.rstrip() + "\n", encoding="utf-8")
            return

    output_path.write_text(generated_block, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a GitHub README patch table from steamdb.csv and local manifest folders."
    )
    parser.add_argument("--csv", dest="csv_path", default="steamdb.csv")
    parser.add_argument("--manifest-root", default="manifest")
    parser.add_argument("--patch-filename", default="Patch.json")
    parser.add_argument("--output", default="README.md")
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    manifest_root = Path(args.manifest_root)
    output_path = Path(args.output)

    rows = load_rows(csv_path)
    generated = build_table_markdown(rows, manifest_root, args.patch_filename)
    write_output(output_path, generated)

    patched_count = sum(is_patched(manifest_root, row["manifest_id"], args.patch_filename) for row in rows)
    print(f"Processed {len(rows)} rows.")
    print(f"Patched manifests found: {patched_count}")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
