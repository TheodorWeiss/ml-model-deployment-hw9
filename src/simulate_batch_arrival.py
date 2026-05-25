"""Имитация поступления daily batch-ей.

Локальный режим:
    python src/simulate_batch_arrival.py --clear --all --interval 60

MinIO/S3 режим:
    python src/simulate_batch_arrival.py --storage minio --clear --all --interval 60

В production новые данные приходили бы из DWH/POS-системы в S3.
В учебном демо мы заранее сгенерированные batch-и публикуем
либо в data/incoming, либо в MinIO bucket inventory-batches.
"""

from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
BATCHES_DIR = DATA_DIR / "daily_batches"
INCOMING_DIR = DATA_DIR / "incoming"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "inventory-batches"


def publish_batch_local(batch_date: str) -> None:
    """Копирует batch из daily_batches в локальную incoming-папку."""
    src_dir = BATCHES_DIR / batch_date
    dst_dir = INCOMING_DIR / batch_date

    if not src_dir.exists():
        raise FileNotFoundError(f"Batch not found: {src_dir}")

    dst_dir.mkdir(parents=True, exist_ok=True)

    for filename in ["inventory.csv", "metadata.json"]:
        src_file = src_dir / filename
        dst_file = dst_dir / filename

        if not src_file.exists():
            raise FileNotFoundError(f"Missing file: {src_file}")

        shutil.copy2(src_file, dst_file)

    print(f"[producer:local] Published batch {batch_date} → {dst_dir}")


def get_s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def publish_batch_minio(batch_date: str) -> None:
    """Загружает batch из daily_batches в MinIO/S3."""
    src_dir = BATCHES_DIR / batch_date

    if not src_dir.exists():
        raise FileNotFoundError(f"Batch not found: {src_dir}")

    s3 = get_s3_client()

    files = {
        "inventory.csv": f"incoming/{batch_date}/inventory.csv",
        "metadata.json": f"incoming/{batch_date}/metadata.json",
    }

    for filename, key in files.items():
        src_file = src_dir / filename

        if not src_file.exists():
            raise FileNotFoundError(f"Missing file: {src_file}")

        s3.upload_file(str(src_file), MINIO_BUCKET, key)
        print(f"[producer:minio] Uploaded {src_file} → s3://{MINIO_BUCKET}/{key}")

    print(f"[producer:minio] Published batch {batch_date}")


def publish_batch(batch_date: str, storage: str) -> None:
    if storage == "local":
        publish_batch_local(batch_date)
    elif storage == "minio":
        publish_batch_minio(batch_date)
    else:
        raise ValueError(f"Unknown storage: {storage}")


def publish_all(storage: str, interval: int) -> None:
    batch_dirs = sorted([p for p in BATCHES_DIR.iterdir() if p.is_dir()])

    if not batch_dirs:
        raise RuntimeError(f"No generated batches found in {BATCHES_DIR}")

    total = len(batch_dirs)

    for i, batch_dir in enumerate(batch_dirs, start=1):
        print(f"[producer] Publishing batch {i}/{total}: {batch_dir.name}")
        publish_batch(batch_dir.name, storage=storage)

        if interval > 0 and i < total:
            print(f"[producer] Waiting {interval} seconds before next batch...")
            time.sleep(interval)


def clear_local_incoming() -> None:
    if INCOMING_DIR.exists():
        shutil.rmtree(INCOMING_DIR)
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[producer:local] Cleared incoming directory: {INCOMING_DIR}")


def clear_minio_incoming() -> None:
    """Удаляет все объекты из MinIO prefix incoming/."""
    s3 = get_s3_client()

    response = s3.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix="incoming/",
    )

    objects = response.get("Contents", [])

    if not objects:
        print("[producer:minio] No objects to delete under incoming/")
        return

    delete_payload = {
        "Objects": [{"Key": obj["Key"]} for obj in objects]
    }

    s3.delete_objects(
        Bucket=MINIO_BUCKET,
        Delete=delete_payload,
    )

    print(f"[producer:minio] Deleted {len(objects)} objects under incoming/")


def clear_storage(storage: str) -> None:
    if storage == "local":
        clear_local_incoming()
    elif storage == "minio":
        clear_minio_incoming()
    else:
        raise ValueError(f"Unknown storage: {storage}")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--storage",
        choices=["local", "minio"],
        default="local",
        help="Where to publish batches: local data/incoming or MinIO/S3",
    )
    parser.add_argument(
        "--date",
        help="Batch date to publish, for example 2026-06-13",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Publish all generated batches in chronological order",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Delay between batches in seconds for --all mode",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear target storage before publishing",
    )
    parser.add_argument(
        "--clear-only",
        action="store_true",
        help="Only clear target storage and exit",
    )

    args = parser.parse_args()

    if args.clear or args.clear_only:
        clear_storage(args.storage)

    if args.clear_only:
        return

    if args.date:
        publish_batch(args.date, storage=args.storage)
    elif args.all:
        publish_all(storage=args.storage, interval=args.interval)
    else:
        raise ValueError("Use --date YYYY-MM-DD, --all, or --clear-only")


if __name__ == "__main__":
    main()