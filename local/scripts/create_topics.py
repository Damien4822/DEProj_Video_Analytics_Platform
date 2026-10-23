"""Create or update Kafka topics from config/pipeline.yaml. Safe to re-run.

    python scripts/create_topics.py            # apply
    python scripts/create_topics.py --dry-run  # show what would change
"""
import argparse
import os
import sys
import time
from pathlib import Path

import yaml
from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
)

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "pipeline.yaml"


def load_config(path):
    cfg = yaml.safe_load(Path(path).read_text())
    defaults = cfg["topics"].get("defaults", {})
    topics = {
        name: {**defaults, **(overrides or {})}
        for name, overrides in cfg["topics"]["items"].items()
    }
    return cfg, topics


def wait_for_broker(admin, timeout_s):
    deadline = time.time() + timeout_s
    while True:
        try:
            return admin.list_topics(timeout=5)
        except KafkaException as e:
            if time.time() > deadline:
                sys.exit(f"broker not reachable: {e}")
            print("waiting for broker...")
            time.sleep(3)


def wait_all(futures, action):
    for name, future in futures.items():
        try:
            future.result()
            print(f"  {action}: {name}")
        except KafkaException as e:
            print(f"  FAILED {action}: {name}: {e}")


def current_retention(admin, name):
    resource = ConfigResource(ConfigResource.Type.TOPIC, name)
    entries = admin.describe_configs([resource])[resource].result()
    return int(entries["retention.ms"].value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--bootstrap", help="override bootstrap servers")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--wait", type=int, default=60, help="seconds to wait for the broker")
    args = parser.parse_args()

    cfg, topics = load_config(args.config)
    bootstrap = (
        args.bootstrap
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or cfg["kafka"]["bootstrap_servers"]
    )
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = wait_for_broker(admin, args.wait).topics
    print(f"connected to {bootstrap}; {len(topics)} topics in config")

    to_create, to_grow, to_alter = [], {}, []
    for name, spec in topics.items():
        if name not in existing:
            to_create.append(NewTopic(
                name,
                num_partitions=spec["partitions"],
                replication_factor=spec["replication_factor"],
                config={"retention.ms": str(spec["retention_ms"])},
            ))
            continue
        have = len(existing[name].partitions)
        if spec["partitions"] > have:
            to_grow[name] = NewPartitions(name, spec["partitions"])
        elif spec["partitions"] < have:
            print(f"  WARN {name}: config has {spec['partitions']} partitions, "
                  f"topic has {have} (Kafka cannot reduce partitions)")
        if current_retention(admin, name) != spec["retention_ms"]:
            to_alter.append(ConfigResource(
                ConfigResource.Type.TOPIC, name,
                incremental_configs=[ConfigEntry(
                    "retention.ms", str(spec["retention_ms"]),
                    incremental_operation=AlterConfigOpType.SET)],
            ))

    if not (to_create or to_grow or to_alter):
        print("nothing to do, topics match config")
        return

    prefix = "would " if args.dry_run else ""
    for t in to_create:
        print(f"  {prefix}create: {t.topic} ({t.num_partitions} partitions)")
    for name, p in to_grow.items():
        print(f"  {prefix}add partitions: {name} -> {p.new_total_count}")
    for r in to_alter:
        print(f"  {prefix}update retention: {r.name}")
    if args.dry_run:
        return

    if to_create:
        wait_all(admin.create_topics(to_create), "created")
    if to_grow:
        wait_all(admin.create_partitions(list(to_grow.values())), "partitions added")
    if to_alter:
        wait_all(admin.incremental_alter_configs(to_alter), "retention updated")


if __name__ == "__main__":
    main()
