"""Bronze -> Silver for one Bronze file (used by silver_flood_processing_dag and by
scripts/replay_bronze_to_silver.py).

For each topic: preprocessing -> cross-batch de-duplication -> schema enforcement ->
Snappy Parquet  datalake_silver/<topic>_<YYYYMMDD_HHMMSS>_from_<bronze stem>.parquet
The manifest line of the Bronze file keeps every data-quality counter (rows in, filtered,
dropped, duplicates, written); the Gold governance DAG reads it to compute the KPIs.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from flood_monitor.preprocessing import (PIPELINE_VERSION, PROCESSORS, drop_already_in_silver,
                                         enforce_schema, remember_in_registry)

logger = logging.getLogger(__name__)


def manifest_path(silver_dir: str) -> Path:
    return Path(silver_dir) / "_manifest.jsonl"


def processed_bronze_files(silver_dir: str) -> set:
    mf = manifest_path(silver_dir)
    if not mf.exists():
        return set()
    return {json.loads(line)["bronze_file"] for line in mf.read_text(encoding="utf-8").splitlines()
            if line.strip()}


def process_bronze_file(b_path: str, silver_dir: str) -> list:
    """Preprocess one Bronze JSON file and write one Parquet file per topic with new rows."""
    os.makedirs(silver_dir, exist_ok=True)
    b_filename = os.path.basename(b_path)
    b_stem = os.path.splitext(b_filename)[0]
    proc_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    with open(b_path, "r", encoding="utf-8") as f:
        raw_payload = json.load(f)
    generated, outputs = [], {}
    for topic, process in PROCESSORS.items():
        df = process(raw_payload, source_filename=b_filename)
        dq = dict(df.attrs.get("dq", {}))
        if df.empty:
            outputs[topic] = {"rows_in": 0, **dq}
            continue
        df, stats = drop_already_in_silver(df, silver_dir, topic)       # cross-batch dedup
        stats = {**dq, **stats}
        if df.empty:
            logger.info("%s: all %d rows already in Silver.", topic, stats["rows_in"])
            outputs[topic] = {**stats, "rows_written": 0}
            continue
        df["processed_at_utc"] = datetime.now(timezone.utc)
        table = enforce_schema(topic, df)
        stats["schema_rejected"] = len(df) - table.num_rows
        table = table.replace_schema_metadata({
            b"bronze_source_file": b_filename.encode(),
            b"pipeline_version": PIPELINE_VERSION.encode(),
            b"preprocessing_stats": json.dumps(stats).encode()})
        out = Path(silver_dir) / f"{topic}_{proc_ts}_from_{b_stem}.parquet"
        tmp = out.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="snappy")
        os.replace(tmp, out)
        remember_in_registry(table, silver_dir, topic)
        generated.append(str(out))
        outputs[topic] = {**stats, "rows_written": table.num_rows, "file": out.name}
        logger.info("Wrote %d rows to %s", table.num_rows, out)
    with manifest_path(silver_dir).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"bronze_file": b_filename, "processed_at_utc": proc_ts,
                             "pipeline_version": PIPELINE_VERSION, "outputs": outputs}) + "\n")
    return generated
