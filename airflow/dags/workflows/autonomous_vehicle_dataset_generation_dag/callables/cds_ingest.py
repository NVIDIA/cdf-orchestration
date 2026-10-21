# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Connect Cosmos-Curator’s output to Cosmos Dataset Search (CDS)"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_CE1_DIR_NAMES = ("ce1_embd_224p_parquet", "ce1_embd_parquet")
_SCALARS = (str, int, float, bool, type(None))


def sanitize_meta(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """JSON-encode non-scalar clip metadata so GA CDS can serialize search hits."""
    sanitized: dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, _SCALARS):
            sanitized[key] = value
        else:
            sanitized[key] = json.dumps(value)
    return sanitized


def normalize_cds_base_url(base_url: str) -> str:
    if not base_url or not str(base_url).strip():
        raise ValueError("cds_url is required")
    cleaned = str(base_url).strip().rstrip("/")
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"cds_url must be an absolute HTTP(S) URL, got: {base_url!r}")
    path = (parsed.path or "").rstrip("/")
    root = f"{parsed.scheme}://{parsed.netloc}"
    if path.endswith("/v1"):
        return f"{root}{path}"
    if path in ("", "/"):
        return f"{root}/v1"
    return f"{root}{path}/v1"


def find_embedding_parquet_dir(base_dir: str | Path) -> Path:
    root = Path(base_dir)
    if not root.is_dir():
        raise FileNotFoundError(f"Curator output dir missing: {root}")
    for name in _CE1_DIR_NAMES:
        path = root / name
        if path.is_dir() and list(path.glob("**/*.parquet")):
            return path
    raise FileNotFoundError(f"No Cosmos-Embed1 parquet directory under {root}")


def _load_sidecar_meta(base_dir: Path, record_id: str) -> dict[str, Any]:
    meta_path = base_dir / "metas" / "v0" / f"{record_id}.json"
    if not meta_path.is_file():
        return {}
    with meta_path.open("r", encoding="utf-8") as handle:
        metadata = json.load(handle)
    if not isinstance(metadata, dict):
        return {}
    span = metadata.get("span_uuid")
    if span is not None and str(span) != record_id:
        raise ValueError(f"UUID mismatch: parquet={record_id} meta.span_uuid={span}")
    return metadata


def curator_rows_to_cds_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    base_dir: str | Path,
) -> list[dict[str, Any]]:
    """Join Curator embedding rows with metadata sidecars into CDS parquet rows."""
    root = Path(base_dir)
    converted: list[dict[str, Any]] = []
    for row in rows:
        record_id = str(row["id"])
        embedding = list(row["embedding"])
        metadata = sanitize_meta(_load_sidecar_meta(root, record_id))
        converted.append(
            {
                "id": record_id,
                "embedding": embedding,
                "$meta": json.dumps(metadata),
            }
        )
    if not converted:
        raise ValueError("Curator export contained no embedding rows")
    return converted


def convert_curator_dir_to_cds_parquet(base_dir: str | Path, output_path: str | Path) -> Path:
    """Write CDS-shaped parquet (id, embedding, $meta) for POST /insert-data."""
    import pandas as pd

    root = Path(base_dir)
    parquet_dir = find_embedding_parquet_dir(root)
    parquet_files = sorted(parquet_dir.glob("**/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files under {parquet_dir}")
    rows: list[dict[str, Any]] = []
    for parquet_path in parquet_files:
        frame = pd.read_parquet(parquet_path)
        if "id" not in frame.columns or "embedding" not in frame.columns:
            raise ValueError(f"{parquet_path} must contain 'id' and 'embedding' columns")
        rows.extend(frame.loc[:, ["id", "embedding"]].to_dict(orient="records"))
    cds_rows = curator_rows_to_cds_rows(rows, base_dir=root)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cds_rows).to_parquet(out, index=False)
    return out


class DatasetSearchClient:
    """Minimal CDS HTTP client for health, collections, document ingest, and search."""

    def __init__(
        self,
        base_url: str,
        session: Any | None = None,
    ) -> None:
        import requests

        self._base_url = normalize_cds_base_url(base_url)
        self._session = session or requests.Session()

    def _root(self) -> str:
        if self._base_url.endswith("/v1"):
            return self._base_url[: -len("/v1")]
        return self._base_url

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if not path.startswith("/"):
            path = f"/{path}"
        response = self._session.request(method, f"{self._base_url}{path}", **kwargs)
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        if "application/json" in response.headers.get("Content-Type", ""):
            return response.json()
        if response.text[:1] in ("{", "["):
            return response.json()
        return response.text

    def health(self) -> str:
        response = self._session.request("GET", f"{self._root()}/health", timeout=30)
        response.raise_for_status()
        return str(response.text).strip().strip('"')

    def list_collections(self) -> list[dict[str, Any]]:
        body = self._request("GET", "/collections", timeout=60)
        if isinstance(body, dict):
            items = body.get("collections", [])
        else:
            items = body
        return [item for item in items or [] if isinstance(item, dict)]

    def create_collection(self, name: str, pipeline: str) -> dict[str, Any]:
        body = self._request(
            "POST",
            "/collections",
            json={
                "name": name,
                "pipeline": pipeline,
                "tags": {},
                "collection_config": {},
                "index_config": {"metric_type": "IP"},
                "metadata_config": {"allow_dynamic_schema": True},
            },
            timeout=120,
        )
        if isinstance(body, dict):
            return body.get("collection", body)
        raise ValueError("Unexpected create-collection response")

    def find_collection(self, name: str) -> str | None:
        """Id of the existing collection called ``name``, or None."""
        for item in self.list_collections():
            if item.get("name") == name:
                found = str(item.get("id") or item.get("collection_id") or "")
                if found:
                    return found
        return None

    def require_pipeline_enabled(self, pipeline: str) -> None:
        """Check CDS has the search pipeline configured and enabled before creating a collection."""
        body = self._request("GET", "/pipelines", timeout=30)
        entries = body if isinstance(body, list) else body.get("pipelines") or []
        for entry in entries:
            if not isinstance(entry, Mapping) or entry.get("id") != pipeline:
                continue
            if entry.get("enabled"):
                return
            missing = entry.get("missing") or []
            raise RuntimeError(
                f"Cosmos Dataset Search pipeline {pipeline!r} is present but not "
                f"enabled; missing configuration: {missing or 'unreported'}. "
                "Collection creation would fail with 'No pipelines have been "
                "initialised'."
            )
        available = [e.get("id") for e in entries if isinstance(e, Mapping) and e.get("id")]
        raise RuntimeError(
            f"Cosmos Dataset Search does not offer pipeline {pipeline!r}. "
            f"Available: {available or 'none'}."
        )

    def resolve_collection(
        self,
        name: str,
        pipeline: str,
    ) -> str:
        """Create the run's collection, reusing it by name on task retries."""
        existing = self.find_collection(name)
        if existing:
            return existing
        created = self.create_collection(name, pipeline)
        resolved = str(created.get("id") or created.get("collection_id") or "")
        if not resolved:
            raise ValueError(f"CDS did not return an id for collection {name!r}")
        return resolved

    def ingest_documents(
        self,
        collection_id: str,
        parquet_path: str | Path,
        *,
        chunk_size: int = 32,
    ) -> str:
        """Send the prepared clip embeddings and metadata into a CDS collection."""
        import pandas as pd

        frame = pd.read_parquet(parquet_path)
        docs: list[dict[str, Any]] = []
        for row in frame.to_dict(orient="records"):
            embedding = [float(value) for value in row["embedding"]]
            if not embedding:
                raise ValueError("Document ingest requires a non-empty embedding")
            document: dict[str, Any] = {
                "mime_type": "application/octet-stream",
                "embedding": embedding,
            }
            if row.get("id") is not None:
                document["id"] = str(row["id"])
            raw_meta = row.get("$meta") or row.get("meta")
            if raw_meta:
                metadata = json.loads(raw_meta) if isinstance(raw_meta, str) else dict(raw_meta)
                if metadata:
                    document["metadata"] = sanitize_meta(metadata)
            docs.append(document)
        if not docs:
            raise ValueError("No embedding rows found in parquet for document ingest")

        for start in range(0, len(docs), chunk_size):
            self._request(
                "POST",
                f"/collections/{collection_id}/documents",
                json=docs[start : start + chunk_size],
                timeout=600,
            )
        return f"documents:{len(docs)}"

    def search_by_embedding(
        self,
        collection_id: str,
        embedding: Sequence[float],
        *,
        top_k: int = 10,
        filters: dict[str, Any] | None = None,
    ) -> Any:
        """Vector embedding search."""
        vector = [float(value) for value in embedding]
        if not vector:
            raise ValueError("embedding query must be non-empty")
        return self._request(
            "POST",
            f"/collections/{collection_id}/search",
            json={
                "top_k": top_k,
                "reconstruct": False,
                "query": [{"embedding": vector}],
                "filters": filters or {},
            },
            timeout=120,
        )


def video_identity(reference: str) -> tuple[str, str]:
    """Extract the parent directory and filename from a video path"""
    cleaned = str(reference).strip().rstrip("/")
    parsed = urlparse(cleaned)
    path = PurePosixPath(parsed.path or cleaned)
    if "." in path.name:
        return path.parent.name, path.name
    return path.name, ""


@dataclass
class QueryEmbedding:
    span_uuid: str
    source_video: str
    vector: list[float]
    vectors: list[list[float]] = field(default_factory=list)


def select_query_embedding(parquet_path: str | Path, query_video: str) -> QueryEmbedding:
    """Find existing embeddings for the query video in Cosmos-Curator’s parquet output."""
    import pandas as pd

    rows = pd.read_parquet(parquet_path).to_dict(orient="records")
    if not rows:
        raise ValueError(f"No embedding rows in {parquet_path}")
    wanted = query_video.strip()
    if not wanted:
        raise ValueError("query_video is required for embedding search")
    want_dir, want_name = video_identity(wanted)
    candidates = []
    for row in rows:
        raw_meta = row.get("$meta") or row.get("meta") or "{}"
        meta = json.loads(raw_meta) if isinstance(raw_meta, str) else dict(raw_meta)
        source = str(meta.get("source_video") or "")
        span = str(row.get("id") or meta.get("span_uuid") or "")
        source_dir, source_name = video_identity(source)
        if span == wanted or (
            want_dir and want_dir == source_dir and (not want_name or want_name == source_name)
        ):
            candidates.append((int(meta.get("start_ns") or 0), span, source, row["embedding"]))
    if not candidates:
        raise ValueError(
            f"No embedding in {parquet_path} matches query {wanted!r}. "
            "Check that Curator produced embeddings for the explicit query and that "
            "its scene directory and camera filename, or span ID, match the metadata."
        )
    _, span, source, vector = min(candidates, key=lambda item: (item[0], item[1]))
    return QueryEmbedding(
        span,
        source,
        [float(v) for v in vector],
        [
            [float(v) for v in item[3]]
            for item in sorted(candidates, key=lambda item: (item[0], item[1]))
        ],
    )


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected s3:// URI, got {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _s3_client(endpoint_url: str | None):
    import boto3

    kwargs: dict[str, Any] = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


def is_ingest_input(relative_key: str) -> bool:
    """Decide whether Curator output should be downloaded for CDS ingestion."""
    head = PurePosixPath(relative_key).parts[0] if relative_key else ""
    if head == "metas":
        return True
    return head in _CE1_DIR_NAMES


def download_files(
    uri: str,
    dest_dir: Path,
    endpoint_url: str | None,
    key_filter: Any = None,
) -> Path:
    """Download matching S3 files into a local directory, or return an existing local path."""
    if not uri.startswith("s3://"):
        return Path(uri)
    bucket, prefix = _parse_s3_uri(uri)
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    client = _s3_client(endpoint_url)
    paginator = client.get_paginator("list_objects_v2")
    dest_dir.mkdir(parents=True, exist_ok=True)
    found = False
    skipped = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents") or []:
            key = str(item["Key"])
            if not key.startswith(prefix):
                continue
            relative = key[len(prefix) :]
            if not relative or key.endswith("/"):
                continue
            if key_filter is not None and not key_filter(relative):
                skipped += 1
                continue
            local_path = dest_dir / relative
            if not local_path.resolve().is_relative_to(dest_dir.resolve()):
                raise ValueError(f"Object path escapes download directory: {key!r}")
            found = True
            local_path.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(local_path))
    if not found:
        raise FileNotFoundError(f"No objects found under {uri}.")
    if skipped:
        logger.info("Downloaded ingest inputs from %s; skipped %s unused objects", uri, skipped)
    return dest_dir


def upload_file(source: Path, uri: str, endpoint_url: str | None) -> str:
    if not uri.startswith("s3://"):
        dest = Path(uri)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(source.read_bytes())
        return str(dest)
    bucket, key = _parse_s3_uri(uri)
    _s3_client(endpoint_url).upload_file(str(source), bucket, key)
    return uri


def retrievals_of(result: Any) -> list[Any]:
    """Hits from a CDS search response, whichever key it used."""
    if isinstance(result, Mapping):
        hits = result.get("retrievals") or result.get("results") or []
    else:
        hits = result or []
    return list(hits) if isinstance(hits, (list, tuple)) else []


def search_when_ready(
    search: Any,
    *,
    timeout_seconds: float = 300,
    interval_seconds: float = 10,
) -> Any:
    """Retry a search until it returns hits."""
    deadline = time.monotonic() + timeout_seconds
    result = search()
    while len(retrievals_of(result)) == 0 and time.monotonic() < deadline:
        time.sleep(interval_seconds)
        result = search()
    if len(retrievals_of(result)) == 0:
        raise RuntimeError("No search hits from CDS.")
    return result


def search_dataset(
    client: DatasetSearchClient,
    collection_id: str,
    query: QueryEmbedding,
    *,
    top_k: int,
    timeout_seconds: float,
) -> Any:
    """Search with CDS using every embedding timeframe of the query video and combines the results."""
    hits = {}
    for vector in query.vectors or [query.vector]:
        response = search_when_ready(
            lambda: client.search_by_embedding(
                collection_id,
                vector,
                top_k=top_k + 1,
                filters=None,
            ),
            timeout_seconds=timeout_seconds,
        )
        for hit in retrievals_of(response):
            meta = hit.get("metadata") or {}
            identity = hit.get("id") or meta.get("span_uuid")
            if not identity:
                raise ValueError("CDS retrieval is missing its span identity.")
            previous = hits.get(identity)
            if previous is None or float(hit.get("score") or 0) > float(previous.get("score") or 0):
                hits[identity] = hit
    return {
        "retrievals": sorted(
            hits.values(), key=lambda hit: float(hit.get("score") or 0), reverse=True
        )
    }


def wait_for_health(client: DatasetSearchClient, timeout_seconds: float = 300) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client.health()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(5)
    raise TimeoutError(f"CDS did not become healthy: {last_error}")


def run(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Read environment configuration and coordinate CDS ingestion and search."""
    import msc_credentials
    import pandas as pd

    # Read the paths, search settings, and service address supplied by the DAG.
    values = dict(os.environ if env is None else env)
    curator_dir = values["CURATOR_DIR"]
    parquet_uri = values["PARQUET_URI"]
    cds_url = values["CDS_URL"]
    collection_name = values["COLLECTION_NAME"]
    pipeline = values.get("PIPELINE", "cosmos_video_search_milvus")
    search_mode = (values.get("SEARCH_MODE") or "embedding").strip().lower()
    top_k = int(values.get("TOP_K") or "10")
    timeout_seconds = float(values.get("POLL_TIMEOUT_SECONDS") or "900")
    work_dir = Path(values.get("WORK_DIR") or "/tmp/cds-ingest")

    # Configure boto3 credentials and the storage endpoint from the MSC secret.
    for key, value in msc_credentials.aws_env_from_msc(
        values.get(msc_credentials.MSC_ENV_VAR, ""), curator_dir
    ).items():
        values.setdefault(key, value)
        os.environ.setdefault(key, value)
    endpoint = values.get("OBJECT_STORE_ENDPOINT") or values.get("S3_ENDPOINT_URL") or None

    # Download corpus metadata and embeddings, then convert them to CDS rows.
    local_curator = download_files(
        curator_dir, work_dir / "curator", endpoint, key_filter=is_ingest_input
    )
    local_parquet = work_dir / "cds_ingest.parquet"
    convert_curator_dir_to_cds_parquet(local_curator, local_parquet)

    # Select query timeframes; a separate query export is not added to the corpus.
    selected_query = None
    query_video = (values.get("QUERY_VIDEO") or "").strip()
    if search_mode == "embedding":
        query_parquet = local_parquet
        query_dir = (values.get("QUERY_CURATOR_DIR") or "").strip()
        if query_dir:
            logger.info("Reading the query video's Curator embeddings from %s", query_dir)
            local_query = download_files(
                query_dir, work_dir / "query_curator", endpoint, key_filter=is_ingest_input
            )
            query_parquet = work_dir / "query_ingest.parquet"
            convert_curator_dir_to_cds_parquet(local_query, query_parquet)
        selected_query = select_query_embedding(query_parquet, query_video)
    span_count = len(pd.read_parquet(local_parquet, columns=["id"]))
    staged = upload_file(local_parquet, parquet_uri, endpoint)

    # Wait for CDS, verify its pipeline, and create or reuse the run's collection.
    client = DatasetSearchClient(cds_url)
    wait_for_health(client)
    client.require_pipeline_enabled(pipeline)
    resolved_id = client.resolve_collection(collection_name, pipeline)

    # Submit corpus vectors and metadata over HTTP, then flush the collection.
    ingest_mode = (values.get("INGEST_MODE") or "documents").strip().lower()
    job_id = client.ingest_documents(resolved_id, local_parquet)
    client._request("POST", f"/admin/collections/{resolved_id}/flush", timeout=300)

    # Record ingestion details for reporting and the downstream collector.
    result: dict[str, Any] = {
        "collection_id": resolved_id,
        "span_count": span_count,
        "job_id": job_id,
        "parquet_uri": staged,
        "embedding_family": "ce1",
        "ingest_mode": ingest_mode,
        "search_mode": search_mode,
    }
    # Search every query timeframe and merge hits; ingest-only runs skip search.
    if search_mode == "embedding":
        assert selected_query is not None
        result["query_video"] = query_video
        result["query_span_uuid"] = selected_query.span_uuid
        result["query_source_video"] = selected_query.source_video
        result["search"] = search_dataset(
            client,
            resolved_id,
            selected_query,
            top_k=top_k,
            timeout_seconds=timeout_seconds,
        )
    if search_mode == "none":
        result["search"] = {"retrievals": []}

    # Save the raw response for collect_results to filter, rank, and resolve clips.
    if "search" in result:
        result["search_hits"] = len(retrievals_of(result["search"]))
        result_prefix = (values.get("RESULT_PREFIX") or "").strip()
        if result_prefix:
            envelope = {
                "query_video": result.get("query_video"),
                "query_span_uuid": result.get("query_span_uuid"),
                "query_source_video": result.get("query_source_video"),
                "search_mode": search_mode,
                "collection_name": collection_name,
                "collection_id": resolved_id,
                "embedding_family": "ce1",
                "ingest_mode": ingest_mode,
                "span_count": span_count,
                "search": result["search"],
            }
            local_raw = work_dir / "cds_search_response.json"
            local_raw.write_text(json.dumps(envelope, indent=2, default=str))
            result["raw_response_uri"] = upload_file(
                local_raw,
                f"{result_prefix.rstrip('/')}/cds_search_response.json",
                endpoint,
            )
    logger.info("Ingest result: %s", json.dumps(result, default=str))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
