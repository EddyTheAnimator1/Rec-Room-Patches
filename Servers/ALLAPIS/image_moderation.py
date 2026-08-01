from __future__ import annotations

import asyncio
import gc
import hashlib
import io
import json
import os
import queue
import subprocess
import sys
import threading
import time
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from PIL import Image, ImageOps, UnidentifiedImageError

import moderation_service
from adapter_data import load_version_json


API_VERSION = "25april2019"
SYSTEM_REPORTER_PLAYER_ID = "00000000-0000-0000-0000-000000000099"
QUARANTINE_DIR_NAME = "IMAGE_QUARANTINE"
IMAGE_DIR_NAME = "IMAGES"
PLAYER_IMAGE_DIR_NAME = "RRPlayer"
BACKEND_IMAGE_DIR_NAME = "RR"
MAX_INPUT_PIXELS = 40_000_000


def utc_now_datetime() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    value = value or utc_now_datetime()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def normalize_image_bytes(
    content: bytes,
    *,
    mime_type: str,
    target_size: tuple[int, int],
) -> tuple[bytes, dict[str, int]]:
    expected_format = "PNG" if mime_type == "image/png" else "JPEG"
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(content), formats=(expected_format,)) as source:
                source.verify()
            with Image.open(io.BytesIO(content), formats=(expected_format,)) as source:
                original_size = source.size
                if source.width <= 0 or source.height <= 0:
                    raise ValueError("Image dimensions must be positive.")
                if source.width * source.height > MAX_INPUT_PIXELS:
                    raise ValueError("Image dimensions are too large.")
                if source.size != target_size:
                    raise ValueError(
                        f"2019 images must be exactly {target_size[0]}x{target_size[1]}."
                    )
                source.load()
                oriented = ImageOps.exif_transpose(source)
                if oriented.size != target_size:
                    raise ValueError(
                        "Image orientation metadata does not match the 2019 image contract."
                    )
                if expected_format == "JPEG":
                    oriented = oriented.convert("RGB")
                elif oriented.mode not in {"RGB", "RGBA"}:
                    oriented = oriented.convert("RGBA")
                clean = Image.new(oriented.mode, target_size)
                clean.paste(oriented)
                output = io.BytesIO()
                if expected_format == "JPEG":
                    clean.save(
                        output,
                        format="JPEG",
                        quality=90,
                        optimize=True,
                        progressive=True,
                        exif=b"",
                        icc_profile=None,
                    )
                else:
                    clean.save(
                        output,
                        format="PNG",
                        optimize=True,
                        icc_profile=None,
                    )
    except (Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise ValueError("Image dimensions are too large.") from None
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError) as exc:
        if isinstance(exc, ValueError) and (
            str(exc)
            in {
                "Image dimensions must be positive.",
                "Image dimensions are too large.",
                "Image orientation metadata does not match the 2019 image contract.",
            }
            or str(exc).startswith("2019 images must be exactly ")
        ):
            raise
        raise ValueError("The uploaded file is not a valid PNG or JPEG image.") from exc
    return output.getvalue(), {
        "original_width": int(original_size[0]),
        "original_height": int(original_size[1]),
        "width": int(target_size[0]),
        "height": int(target_size[1]),
    }


class NsfwJsEngine:
    def __init__(self, script_path: Path, *, timeout_seconds: int = 90):
        self.script_path = script_path.resolve()
        self.timeout_seconds = timeout_seconds
        self._process: subprocess.Popen[str] | None = None
        self._responses: queue.Queue[dict[str, Any]] = queue.Queue()
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def _start(self) -> None:
        if self._process is not None and self._process.poll() is None:
            return
        if not self.script_path.is_file():
            raise RuntimeError(f"NSFWJS worker is missing: {self.script_path}")
        self.close()
        env = dict(os.environ)
        env.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
        self._process = subprocess.Popen(
            ["node", str(self.script_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env=env,
        )
        threading.Thread(target=self._read_stdout, daemon=True).start()

    def _read_stdout(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        for line in process.stdout:
            try:
                payload = json.loads(line)
            except (TypeError, ValueError):
                continue
            if isinstance(payload, dict) and payload.get("id"):
                self._responses.put(payload)

    def classify(self, image_path: Path) -> dict[str, float]:
        with self._lock:
            self._start()
            process = self._process
            if process is None or process.stdin is None:
                raise RuntimeError("NSFWJS worker did not start.")
            request_id = str(uuid.uuid4())
            process.stdin.write(
                json.dumps(
                    {"id": request_id, "path": str(image_path.resolve())},
                    separators=(",", ":"),
                )
                + "\n"
            )
            process.stdin.flush()
            expires_at = time.monotonic() + self.timeout_seconds
            while True:
                remaining = expires_at - time.monotonic()
                if remaining <= 0:
                    self.close()
                    raise TimeoutError("NSFWJS classification timed out.")
                try:
                    response = self._responses.get(timeout=remaining)
                except queue.Empty:
                    self.close()
                    raise TimeoutError("NSFWJS classification timed out.") from None
                if response.get("id") != request_id:
                    continue
                if not response.get("ok"):
                    raise RuntimeError(
                        str(response.get("error") or "NSFWJS classification failed.")
                    )
                scores = response.get("scores")
                if not isinstance(scores, dict):
                    raise RuntimeError("NSFWJS returned an invalid score payload.")
                return {str(key): float(value) for key, value in scores.items()}

    def close(self) -> None:
        process = self._process
        self._process = None
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


class ImageModerationManager:
    def __init__(
        self,
        context: Any,
        *,
        root_dir: Path,
        on_approved: Callable[[dict[str, Any]], Awaitable[None]],
    ):
        self.context = context
        self.db = context.db
        self.root_dir = root_dir.resolve()
        self.config = load_version_json(API_VERSION, "image_moderation.json", dict)
        self.on_approved = on_approved
        self.instance_id = str(uuid.uuid4())
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._nudenet: Any | None = None
        self._model_last_used_at: float | None = None
        self._nsfwjs = NsfwJsEngine(self.root_dir / "tools" / "nsfwjs_worker.js")

    @property
    def target_size(self) -> tuple[int, int]:
        return (
            int(self.config["target_width"]),
            int(self.config["target_height"]),
        )

    def register_job(
        self,
        conn: Any,
        *,
        asset_id: str,
        owner_player_id: str,
        purpose: str,
        activation_type: str,
        activation: dict[str, Any] | None,
    ) -> str:
        job_id = str(uuid.uuid4())
        now = utc_now_datetime()
        available_at = now + timedelta(
            seconds=max(0, int(self.config.get("queue_delay_seconds", 3)))
        )
        conn.execute(
            """
            INSERT INTO image_moderation_jobs(
                job_id, asset_id, api_version, owner_player_id, purpose,
                activation_type, activation_json, status, decision, attempts,
                next_attempt_at, lease_owner, lease_expires_at,
                nudenet_json, nsfwjs_json, max_nudenet_score,
                max_nsfwjs_score, moderation_case_id, player_case_id,
                last_error, created_at, updated_at, reviewed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 'pending', 0, ?, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL)
            """,
            (
                job_id,
                asset_id,
                API_VERSION,
                owner_player_id,
                purpose,
                activation_type,
                json.dumps(activation or {}, sort_keys=True),
                utc_text(available_at),
                utc_text(now),
                utc_text(now),
            ),
        )
        return job_id

    def wake(self) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._wake.set)

    async def start(self) -> None:
        if not bool(self.config.get("enabled", True)) or self._task is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(
            self._run(), name="image-moderation-worker"
        )

    async def close(self) -> None:
        self._stop.set()
        self._wake.set()
        task = self._task
        self._task = None
        if task is not None:
            await task
        await asyncio.to_thread(self._release_models_if_idle, force=True)

    async def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            try:
                job = await asyncio.to_thread(self._claim_job)
            except Exception as exc:
                print(f"Image moderation queue claim failed: {exc}", file=sys.stderr)
                job = None
            if job is not None:
                try:
                    await self._process(job)
                except Exception as exc:
                    await asyncio.to_thread(self._retry_job, job, exc)
                continue
            await asyncio.to_thread(self._release_models_if_idle)
            try:
                await asyncio.wait_for(
                    self._wake.wait(),
                    timeout=await asyncio.to_thread(self._next_wait_seconds),
                )
            except asyncio.TimeoutError:
                pass

    def _next_wait_seconds(self) -> float:
        idle_seconds = max(
            1.0, float(self.config.get("idle_poll_interval_seconds", 30))
        )
        with self.db.connection() as conn:
            row = conn.execute(
                """
                SELECT MIN(next_attempt_at) AS next_attempt_at
                FROM image_moderation_jobs
                WHERE status IN ('pending', 'retry', 'activating', 'reporting')
                """
            ).fetchone()
        if row is None or not row["next_attempt_at"]:
            return idle_seconds
        try:
            due = datetime.fromisoformat(
                str(row["next_attempt_at"]).replace("Z", "+00:00")
            )
        except ValueError:
            return 1.0
        return min(idle_seconds, max(0.25, (due - utc_now_datetime()).total_seconds()))

    def _claim_job(self) -> dict[str, Any] | None:
        now = utc_now_datetime()
        now_text = utc_text(now)
        lease_expires = utc_text(
            now + timedelta(seconds=max(30, int(self.config.get("lease_seconds", 180))))
        )
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = CASE
                        WHEN decision = 'safe' THEN 'activating'
                        WHEN decision = 'rejected' THEN 'reporting'
                        ELSE 'retry'
                    END,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE status = 'processing'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                """,
                (now_text, now_text),
            )
            row = conn.execute(
                """
                SELECT * FROM image_moderation_jobs
                WHERE status IN ('pending', 'retry', 'activating', 'reporting')
                  AND next_attempt_at <= ?
                  AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                ORDER BY created_at, job_id
                LIMIT 1
                """,
                (now_text, now_text),
            ).fetchone()
            if row is None:
                return None
            original_status = str(row["status"])
            claimed_status = (
                original_status
                if original_status in {"activating", "reporting"}
                else "processing"
            )
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = ?, lease_owner = ?, lease_expires_at = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (
                    claimed_status,
                    self.instance_id,
                    lease_expires,
                    now_text,
                    str(row["job_id"]),
                ),
            )
        result = dict(row)
        result["status"] = claimed_status
        return result

    async def _process(self, job: dict[str, Any]) -> None:
        decision = str(job.get("decision") or "pending")
        if decision == "safe":
            await self._activate(job)
            return
        if decision == "rejected":
            await self._report_rejection(job)
            return
        asset = self._asset_for_job(job)
        image_path = (self.context.data_dir / str(asset["relative_path"])).resolve()
        quarantine_root = (self.context.data_dir / QUARANTINE_DIR_NAME).resolve()
        if quarantine_root not in image_path.parents or not image_path.is_file():
            raise FileNotFoundError("Queued image is missing from quarantine.")
        evaluation = await asyncio.to_thread(self._evaluate, image_path)
        if evaluation["rejected"]:
            await asyncio.to_thread(self._record_rejected, job, evaluation)
            job.update(evaluation)
            job["decision"] = "rejected"
            await self._report_rejection(job)
        else:
            await asyncio.to_thread(self._publish_safe, job, asset, evaluation)
            job.update(evaluation)
            job["decision"] = "safe"
            await self._activate(job)

    def _asset_for_job(self, job: dict[str, Any]) -> dict[str, Any]:
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT * FROM data_assets WHERE asset_id = ?",
                (str(job["asset_id"]),),
            ).fetchone()
        if row is None:
            raise FileNotFoundError("Queued image asset no longer exists.")
        return dict(row)

    def _evaluate(self, image_path: Path) -> dict[str, Any]:
        self._model_last_used_at = time.monotonic()
        if self._nudenet is None:
            from nudenet import NudeDetector

            self._nudenet = NudeDetector()
        views = [("published", image_path)]
        detections: list[dict[str, Any]] = []
        nsfwjs_views: dict[str, dict[str, float]] = {}
        nsfwjs_scores: dict[str, float] = {}
        for view_name, view_path in views:
            view_detections = self._nudenet.detect(str(view_path))
            if not isinstance(view_detections, list):
                raise RuntimeError("NudeNet returned an invalid detection payload.")
            for item in view_detections:
                if isinstance(item, dict):
                    detections.append({**item, "view": view_name})
            view_scores = self._nsfwjs.classify(view_path)
            nsfwjs_views[view_name] = view_scores
            for class_name, score in view_scores.items():
                nsfwjs_scores[class_name] = max(
                    nsfwjs_scores.get(class_name, 0.0), float(score)
                )
        nude_thresholds = {
            str(key): float(value)
            for key, value in self.config["nudenet_320n"]["rejected_classes"].items()
        }
        nsfw_thresholds = {
            str(key): float(value)
            for key, value in self.config["nsfwjs_mobilenet_v2"][
                "rejected_classes"
            ].items()
        }
        rejected_detections = [
            item
            for item in detections
            if isinstance(item, dict)
            and str(item.get("class")) in nude_thresholds
            and float(item.get("score") or 0.0)
            >= nude_thresholds[str(item.get("class"))]
        ]
        rejected_predictions = {
            name: score
            for name, score in nsfwjs_scores.items()
            if name in nsfw_thresholds and score >= nsfw_thresholds[name]
        }
        max_nudenet = max(
            (
                float(item.get("score") or 0.0)
                for item in detections
                if isinstance(item, dict)
                and str(item.get("class")) in nude_thresholds
            ),
            default=0.0,
        )
        max_nsfwjs = max(
            (nsfwjs_scores.get(name, 0.0) for name in nsfw_thresholds),
            default=0.0,
        )
        rejected = bool(rejected_detections or rejected_predictions)
        very_config = self.config["very_confident"]
        nude_high = max_nudenet >= float(very_config["minimum_nudenet_score"])
        nsfw_high = max_nsfwjs >= float(very_config["minimum_nsfwjs_score"])
        very_confident = rejected and (
            nude_high and nsfw_high
            if bool(very_config.get("require_both_models", True))
            else nude_high or nsfw_high
        )
        confirmation = very_config.get("cross_model_confirmation")
        if rejected and isinstance(confirmation, dict):
            confirmed_nude_classes = {
                str(value) for value in confirmation.get("nudenet_classes", [])
            }
            confirmed_nsfw_classes = {
                str(value) for value in confirmation.get("nsfwjs_classes", [])
            }
            confirmed_nude_score = max(
                (
                    float(item.get("score") or 0.0)
                    for item in detections
                    if str(item.get("class")) in confirmed_nude_classes
                ),
                default=0.0,
            )
            confirmed_nsfw_score = max(
                (nsfwjs_scores.get(name, 0.0) for name in confirmed_nsfw_classes),
                default=0.0,
            )
            very_confident = very_confident or (
                confirmed_nude_score
                >= float(confirmation["minimum_nudenet_score"])
                and confirmed_nsfw_score
                >= float(confirmation["minimum_nsfwjs_score"])
            )
        self._model_last_used_at = time.monotonic()
        return {
            "rejected": rejected,
            "very_confident": very_confident,
            "nudenet": detections,
            "nsfwjs": nsfwjs_scores,
            "nsfwjs_views": nsfwjs_views,
            "max_nudenet_score": max_nudenet,
            "max_nsfwjs_score": max_nsfwjs,
        }

    def _release_models_if_idle(self, *, force: bool = False) -> bool:
        last_used = self._model_last_used_at
        idle_seconds = max(
            0.0,
            float(self.config.get("model_idle_timeout_seconds", 30)),
        )
        if not force and (
            last_used is None or time.monotonic() - last_used < idle_seconds
        ):
            return False
        had_nudenet = self._nudenet is not None
        had_nsfwjs = self._nsfwjs.is_running
        self._nudenet = None
        self._nsfwjs.close()
        self._model_last_used_at = None
        if had_nudenet:
            gc.collect()
        return had_nudenet or had_nsfwjs

    def _record_rejected(
        self, job: dict[str, Any], evaluation: dict[str, Any]
    ) -> None:
        now = utc_text()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = 'reporting', decision = 'rejected',
                    nudenet_json = ?, nsfwjs_json = ?,
                    max_nudenet_score = ?, max_nsfwjs_score = ?,
                    lease_owner = NULL, lease_expires_at = NULL,
                    next_attempt_at = ?, last_error = NULL,
                    updated_at = ?, reviewed_at = ?
                WHERE job_id = ?
                """,
                (
                    json.dumps(evaluation["nudenet"], sort_keys=True),
                    json.dumps(
                        {
                            "maximum_scores": evaluation["nsfwjs"],
                            "views": evaluation.get("nsfwjs_views", {}),
                        },
                        sort_keys=True,
                    ),
                    float(evaluation["max_nudenet_score"]),
                    float(evaluation["max_nsfwjs_score"]),
                    now,
                    now,
                    now,
                    str(job["job_id"]),
                ),
            )

    def _publish_safe(
        self,
        job: dict[str, Any],
        asset: dict[str, Any],
        evaluation: dict[str, Any],
    ) -> None:
        source = (self.context.data_dir / str(asset["relative_path"])).resolve()
        bucket = (
            PLAYER_IMAGE_DIR_NAME
            if str(asset.get("owner_player_id") or "")
            else BACKEND_IMAGE_DIR_NAME
        )
        filename = Path(str(asset["relative_path"])).name
        destination = (
            self.context.data_dir / IMAGE_DIR_NAME / bucket / filename
        ).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_file():
            os.replace(source, destination)
        elif not destination.is_file():
            raise FileNotFoundError("Approved image disappeared before publication.")
        relative_path = f"{IMAGE_DIR_NAME}/{bucket}/{filename}"
        now = utc_text()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE data_assets SET relative_path = ? WHERE asset_id = ?",
                (relative_path, str(job["asset_id"])),
            )
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = 'activating', decision = 'safe',
                    nudenet_json = ?, nsfwjs_json = ?,
                    max_nudenet_score = ?, max_nsfwjs_score = ?,
                    lease_owner = NULL, lease_expires_at = NULL,
                    next_attempt_at = ?, last_error = NULL,
                    updated_at = ?, reviewed_at = ?
                WHERE job_id = ?
                """,
                (
                    json.dumps(evaluation["nudenet"], sort_keys=True),
                    json.dumps(
                        {
                            "maximum_scores": evaluation["nsfwjs"],
                            "views": evaluation.get("nsfwjs_views", {}),
                        },
                        sort_keys=True,
                    ),
                    float(evaluation["max_nudenet_score"]),
                    float(evaluation["max_nsfwjs_score"]),
                    now,
                    now,
                    now,
                    str(job["job_id"]),
                ),
            )

    async def _activate(self, job: dict[str, Any]) -> None:
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT * FROM image_moderation_jobs WHERE job_id = ?",
                (str(job["job_id"]),),
            ).fetchone()
        if row is None:
            return
        payload = dict(row)
        try:
            payload["activation"] = json.loads(payload["activation_json"] or "{}")
        except (TypeError, ValueError):
            payload["activation"] = {}
        await self.on_approved(payload)
        now = utc_text()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = 'approved', lease_owner = NULL,
                    lease_expires_at = NULL, last_error = NULL, updated_at = ?
                WHERE job_id = ?
                """,
                (now, str(job["job_id"])),
            )

    async def _report_rejection(self, job: dict[str, Any]) -> None:
        case_result = await asyncio.to_thread(self._create_rejection_cases, job)
        now = utc_text()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = 'rejected', moderation_case_id = ?, player_case_id = ?,
                    lease_owner = NULL, lease_expires_at = NULL,
                    last_error = NULL, updated_at = ?
                WHERE job_id = ?
                """,
                (
                    case_result.get("image_case_id"),
                    case_result.get("player_case_id"),
                    now,
                    str(job["job_id"]),
                ),
            )
        if case_result.get("timed_out") and self.context.transient is not None:
            owner_player_id = str(job.get("owner_player_id") or "")
            await self.context.transient.revoke_player_transient_state(
                owner_player_id,
                aliases=self.context.transient_player_aliases(owner_player_id),
            )

    def _create_rejection_cases(self, job: dict[str, Any]) -> dict[str, Any]:
        with self.db.connection() as conn:
            current = conn.execute(
                "SELECT * FROM image_moderation_jobs WHERE job_id = ?",
                (str(job["job_id"]),),
            ).fetchone()
        if current is None:
            raise KeyError(str(job["job_id"]))
        current = dict(current)
        nudenet = json.loads(current.get("nudenet_json") or "[]")
        nsfwjs = json.loads(current.get("nsfwjs_json") or "{}")
        raw = json.dumps(
            {
                "asset_id": current["asset_id"],
                "owner_player_id": current["owner_player_id"],
                "purpose": current["purpose"],
                "nudenet_320n": nudenet,
                "nsfwjs_mobilenet_v2": nsfwjs,
                "max_nudenet_score": current["max_nudenet_score"],
                "max_nsfwjs_score": current["max_nsfwjs_score"],
                "sha256": self._asset_sha256(str(current["asset_id"])),
            },
            sort_keys=True,
        )
        image_case_id = str(current.get("moderation_case_id") or "")
        if not image_case_id:
            report = self.context.create_moderation_report(
                reporter_player_id=SYSTEM_REPORTER_PLAYER_ID,
                target_type="image",
                target_id=str(current["asset_id"]),
                canonical_category="sexual_content",
                raw_category={"automated": "dual_model_nudity"},
                category_schema="image-moderation-v1",
                public_details="Automated image moderation rejected this upload.",
                raw_details=raw,
                room_id=None,
                game_session_id=None,
                source_version=API_VERSION,
                source_endpoint="image_moderation_worker",
                source_schema="nudenet-320n+nsfwjs-mobilenet-v2",
                source_payload={"job_id": str(current["job_id"])},
                evidence_status="restricted",
            )
            image_case_id = str(report["case_id"])
            with self.db.transaction() as conn:
                conn.execute(
                    "UPDATE image_moderation_jobs SET moderation_case_id = ? WHERE job_id = ?",
                    (image_case_id, str(current["job_id"])),
                )
        if not moderation_service.is_content_control_active(
            self.db,
            target_type="image",
            target_id=str(current["asset_id"]),
        ):
            moderation_service.transition_case(
                self.db,
                case_id=image_case_id,
                action="quarantine",
                actor_id="image-moderation",
                reason="Rejected by NudeNet 320n or NSFWJS MobileNetV2.",
                idempotency_key=f"image-moderation:quarantine:{current['asset_id']}",
            )
        very = self.config["very_confident"]
        nude_high = float(current.get("max_nudenet_score") or 0.0) >= float(
            very["minimum_nudenet_score"]
        )
        nsfw_high = float(current.get("max_nsfwjs_score") or 0.0) >= float(
            very["minimum_nsfwjs_score"]
        )
        very_confident = (
            nude_high and nsfw_high
            if bool(very.get("require_both_models", True))
            else nude_high or nsfw_high
        )
        confirmation = very.get("cross_model_confirmation")
        if isinstance(confirmation, dict):
            confirmed_nude_classes = {
                str(value) for value in confirmation.get("nudenet_classes", [])
            }
            confirmed_nsfw_classes = {
                str(value) for value in confirmation.get("nsfwjs_classes", [])
            }
            maximum_nsfwjs = (
                nsfwjs.get("maximum_scores", nsfwjs)
                if isinstance(nsfwjs, dict)
                else {}
            )
            if not isinstance(maximum_nsfwjs, dict):
                maximum_nsfwjs = {}
            confirmed_nude_score = max(
                (
                    float(item.get("score") or 0.0)
                    for item in nudenet
                    if isinstance(item, dict)
                    and str(item.get("class")) in confirmed_nude_classes
                ),
                default=0.0,
            )
            confirmed_nsfw_score = max(
                (
                    float(maximum_nsfwjs.get(name) or 0.0)
                    for name in confirmed_nsfw_classes
                ),
                default=0.0,
            )
            very_confident = very_confident or (
                confirmed_nude_score
                >= float(confirmation["minimum_nudenet_score"])
                and confirmed_nsfw_score
                >= float(confirmation["minimum_nsfwjs_score"])
            )
        player_case_id = str(current.get("player_case_id") or "") or None
        timed_out = False
        owner_player_id = str(current.get("owner_player_id") or "")
        if very_confident and owner_player_id != SYSTEM_REPORTER_PLAYER_ID:
            if player_case_id is None:
                player_report = self.context.create_moderation_report(
                    reporter_player_id=SYSTEM_REPORTER_PLAYER_ID,
                    target_type="player",
                    target_id=owner_player_id,
                    canonical_category="sexual_content",
                    raw_category={"automated": "very_confident_nudity_upload"},
                    category_schema="image-moderation-v1",
                    public_details="Very-high-confidence explicit image upload.",
                    raw_details=raw,
                    room_id=None,
                    game_session_id=None,
                    source_version=API_VERSION,
                    source_endpoint="image_moderation_worker",
                    source_schema="nudenet-320n+nsfwjs-mobilenet-v2",
                    source_payload={
                        "job_id": str(current["job_id"]),
                        "asset_id": str(current["asset_id"]),
                    },
                    evidence_status="restricted",
                )
                player_case_id = str(player_report["case_id"])
                with self.db.transaction() as conn:
                    conn.execute(
                        "UPDATE image_moderation_jobs SET player_case_id = ? WHERE job_id = ?",
                        (player_case_id, str(current["job_id"])),
                    )
            try:
                moderation_service.transition_case(
                    self.db,
                    case_id=player_case_id,
                    action="timeout",
                    actor_id="image-moderation",
                    reason="Very-high-confidence explicit image upload.",
                    duration_seconds=int(very["timeout_seconds"]),
                    idempotency_key=f"image-moderation:timeout:{current['asset_id']}",
                )
                timed_out = True
            except ValueError as exc:
                if not any(
                    text in str(exc)
                    for text in ("active account sanction", "Coach cannot receive")
                ):
                    raise
        return {
            "image_case_id": image_case_id,
            "player_case_id": player_case_id,
            "timed_out": timed_out,
        }

    def _asset_sha256(self, asset_id: str) -> str:
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT relative_path FROM data_assets WHERE asset_id = ?",
                (asset_id,),
            ).fetchone()
        if row is None:
            return ""
        path = (self.context.data_dir / str(row["relative_path"])).resolve()
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""

    def _retry_job(self, job: dict[str, Any], exc: Exception) -> None:
        attempts = int(job.get("attempts") or 0) + 1
        delay = min(
            max(1, int(self.config.get("retry_max_seconds", 300))),
            2 ** min(attempts, 8),
        )
        decision = str(job.get("decision") or "pending")
        status = (
            "activating"
            if decision == "safe"
            else "reporting"
            if decision == "rejected"
            else "retry"
        )
        now = utc_now_datetime()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE image_moderation_jobs
                SET status = ?, attempts = ?, next_attempt_at = ?,
                    lease_owner = NULL, lease_expires_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (
                    status,
                    attempts,
                    utc_text(now + timedelta(seconds=delay)),
                    f"{type(exc).__name__}: {exc}"[:2000],
                    utc_text(now),
                    str(job["job_id"]),
                ),
            )
        print(
            f"Image moderation job {job['job_id']} will retry: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

    def status(self) -> dict[str, Any]:
        with self.db.connection() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM image_moderation_jobs
                GROUP BY status
                """
            ).fetchall()
        return {
            "enabled": bool(self.config.get("enabled", True)),
            "running": self._task is not None and not self._task.done(),
            "models_resident": self._nudenet is not None or self._nsfwjs.is_running,
            "jobs": {str(row["status"]): int(row["count"]) for row in rows},
        }
