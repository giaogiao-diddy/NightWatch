import logging
import math
import re
from hashlib import blake2b
from typing import Any

try:
    from rank_bm25 import BM25Okapi
except ImportError:  # pragma: no cover - optional runtime dependency
    BM25Okapi = None  # type: ignore[assignment]

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qdrant_models
except ImportError:  # pragma: no cover - optional runtime dependency
    QdrantClient = None  # type: ignore[assignment]
    qdrant_models = None  # type: ignore[assignment]

from nightwatch.config import get_settings
from nightwatch.models.incident import IncidentEvent


logger = logging.getLogger(__name__)

TOKEN_PATTERN = re.compile(r"[a-z0-9_.:-]+")
EMBEDDING_DIMENSION = 64
DEFAULT_COLLECTION = "nightwatch_incident_memory"

DEFAULT_CASE_MEMORY: list[dict[str, Any]] = [
    {
        "case_id": "spark_oom_memory_tuning",
        "title": "Spark executor OOM due to memory pressure",
        "symptoms": [
            "ExecutorLostFailure",
            "Container killed by YARN",
            "OutOfMemoryError",
        ],
        "root_cause": "Executor memory is insufficient for current shuffle payload.",
        "recommended_action": "Increase executor memory and retry with controlled shuffle partitions.",
        "evidence": ["ExecutorLostFailure", "OutOfMemoryError"],
    },
    {
        "case_id": "spark_skew_partition_hotspot",
        "title": "Spark skew causes oversized reducer partitions",
        "symptoms": [
            "skewed partition",
            "imbalanced shuffle",
            "long-tail reducer",
        ],
        "root_cause": "One or more partitions dominate shuffle processing due to key skew.",
        "recommended_action": "Enable skew mitigation and rebalance shuffle partitions before retry.",
        "evidence": ["skewed partition", "imbalanced shuffle"],
    },
    {
        "case_id": "flink_schema_drift_circuit_break",
        "title": "Flink CDC schema drift requires isolation",
        "symptoms": [
            "unsupported cdc data type",
            "column type mismatch",
            "schema drift",
        ],
        "root_cause": "Upstream DDL changed and broke CDC compatibility contract.",
        "recommended_action": "Pause CDC job and suspend downstream DAGs, then notify owners.",
        "evidence": ["unsupported cdc data type", "schema drift"],
    },
    {
        "case_id": "spark_driver_contention_gc",
        "title": "Spark driver contention with GC pressure",
        "symptoms": [
            "driver is not responsive",
            "gc overhead limit exceeded",
            "driver contention",
        ],
        "root_cause": "Driver thread contention and garbage collection overhead stall scheduling.",
        "recommended_action": "Restart workflow with guarded parallelism and driver-focused resource tuning.",
        "evidence": ["gc overhead limit exceeded", "driver contention"],
    },
]


def _tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []

    max_value = max(values)
    min_value = min(values)
    if math.isclose(max_value, min_value):
        return [1.0 if value > 0 else 0.0 for value in values]

    return [(value - min_value) / (max_value - min_value) for value in values]


def _hash_to_slot(token: str) -> tuple[int, float]:
    digest = blake2b(token.encode("utf-8"), digest_size=8).digest()
    slot = int.from_bytes(digest[:4], byteorder="little", signed=False) % EMBEDDING_DIMENSION
    polarity = 1.0 if (digest[4] % 2 == 0) else -1.0
    return slot, polarity


def _embed_text(text: str) -> list[float]:
    vector = [0.0] * EMBEDDING_DIMENSION
    for token in _tokenize(text):
        slot, polarity = _hash_to_slot(token)
        vector[slot] += polarity

    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def _build_case_text(case: dict[str, Any]) -> str:
    symptoms = " ".join(str(item) for item in case.get("symptoms", []))
    evidence = " ".join(str(item) for item in case.get("evidence", []))
    return " ".join(
        [
            str(case.get("title", "")),
            symptoms,
            str(case.get("root_cause", "")),
            str(case.get("recommended_action", "")),
            evidence,
        ]
    ).strip()


def _build_memory_cases(incident: IncidentEvent | None = None) -> list[dict[str, Any]]:
    cases = [dict(case) for case in DEFAULT_CASE_MEMORY]
    if incident is None:
        return cases

    incident_case = {
        "case_id": f"incident_context_{incident.incident_id}",
        "title": f"Incident context for {incident.job_name}",
        "symptoms": [incident.error_signature],
        "root_cause": "Context-only reference generated from latest incident payload.",
        "recommended_action": "Use with caution and prioritize historical validated memories.",
        "evidence": [snippet.content for snippet in incident.log_snippets[:2]],
    }
    cases.append(incident_case)
    return cases


def _compute_bm25_scores(query: str, documents: list[str]) -> list[float]:
    query_tokens = _tokenize(query)
    if not query_tokens:
        return [0.0] * len(documents)

    tokenized_documents = [_tokenize(document) for document in documents]

    if BM25Okapi is None:
        logger.warning("rank_bm25 is not installed; fallback to token-overlap lexical scoring")
        overlap_scores: list[float] = []
        query_token_set = set(query_tokens)
        for tokens in tokenized_documents:
            token_set = set(tokens)
            overlap = len(query_token_set & token_set)
            overlap_scores.append(float(overlap))
        return overlap_scores

    bm25 = BM25Okapi(tokenized_documents)
    raw_scores = bm25.get_scores(query_tokens)
    return [float(score) for score in raw_scores]


def _ensure_qdrant_collection(client: Any, collection_name: str) -> None:
    if qdrant_models is None:
        return

    existing = {item.name for item in client.get_collections().collections}
    if collection_name in existing:
        return

    client.create_collection(
        collection_name=collection_name,
        vectors_config=qdrant_models.VectorParams(
            size=EMBEDDING_DIMENSION,
            distance=qdrant_models.Distance.COSINE,
        ),
    )


def _upsert_cases_to_qdrant(
    client: Any,
    collection_name: str,
    cases: list[dict[str, Any]],
    documents: list[str],
) -> None:
    if qdrant_models is None:
        return

    points = [
        qdrant_models.PointStruct(
            id=index + 1,
            vector=_embed_text(documents[index]),
            payload={
                "case_id": case.get("case_id", f"case_{index + 1}"),
                "title": case.get("title", ""),
                "root_cause": case.get("root_cause", ""),
                "recommended_action": case.get("recommended_action", ""),
                "evidence": case.get("evidence", []),
                "memory_text": documents[index],
            },
        )
        for index, case in enumerate(cases)
    ]
    client.upsert(collection_name=collection_name, points=points, wait=True)


def _query_vector_scores(
    query: str,
    cases: list[dict[str, Any]],
    documents: list[str],
    collection_name: str,
    top_k: int,
) -> dict[str, float]:
    if QdrantClient is None or qdrant_models is None:
        logger.warning("qdrant-client is not installed; vector retrieval skipped")
        return {}

    settings = get_settings()
    client_options: dict[str, Any] = {
        "timeout": settings.qdrant_timeout_seconds,
    }
    if settings.qdrant_api_key.strip():
        client_options["api_key"] = settings.qdrant_api_key.strip()

    if settings.qdrant_url.strip():
        client = QdrantClient(url=settings.qdrant_url.strip(), **client_options)
    else:
        client = QdrantClient(path=settings.qdrant_path, **client_options)

    _ensure_qdrant_collection(client, collection_name=collection_name)
    _upsert_cases_to_qdrant(
        client=client,
        collection_name=collection_name,
        cases=cases,
        documents=documents,
    )

    vector_query = _embed_text(query)
    search_limit = max(top_k * 2, top_k)

    limit = min(search_limit, len(cases))
    if hasattr(client, "search"):
        scored_hits = client.search(
            collection_name=collection_name,
            query_vector=vector_query,
            limit=limit,
            with_payload=True,
        )
    else:
        query_result = client.query_points(
            collection_name=collection_name,
            query=vector_query,
            limit=limit,
            with_payload=True,
        )
        scored_hits = getattr(query_result, "points", query_result)

    vector_scores: dict[str, float] = {}
    for hit in scored_hits:
        payload = hit.payload or {}
        case_id = str(payload.get("case_id", ""))
        if not case_id:
            continue
        vector_scores[case_id] = float(hit.score)
    return vector_scores


def hybrid_retrieve(
    query: str,
    incident: IncidentEvent | None = None,
    top_k: int = 5,
    collection_name: str = DEFAULT_COLLECTION,
) -> list[dict[str, Any]]:
    if not query.strip():
        return []
    if top_k < 1:
        raise ValueError("top_k must be at least 1")

    cases = _build_memory_cases(incident=incident)
    if not cases:
        return []

    documents = [_build_case_text(case) for case in cases]
    bm25_raw_scores = _compute_bm25_scores(query=query, documents=documents)
    bm25_scores = _normalize(bm25_raw_scores)

    try:
        vector_raw_score_map = _query_vector_scores(
            query=query,
            cases=cases,
            documents=documents,
            collection_name=collection_name,
            top_k=top_k,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("vector retrieval failed; fallback to lexical-only ranking", exc_info=exc)
        vector_raw_score_map = {}

    vector_raw_scores = [vector_raw_score_map.get(str(case.get("case_id", "")), 0.0) for case in cases]
    vector_scores = _normalize(vector_raw_scores)

    fused_hits: list[dict[str, Any]] = []
    for index, case in enumerate(cases):
        case_id = str(case.get("case_id", f"case_{index + 1}"))
        lexical_score = bm25_scores[index]
        vector_score = vector_scores[index]
        fused_score = (0.6 * lexical_score) + (0.4 * vector_score)

        fused_hits.append(
            {
                "case_id": case_id,
                "title": str(case.get("title", "")),
                "root_cause": str(case.get("root_cause", "")),
                "recommended_action": str(case.get("recommended_action", "")),
                "evidence": [str(item) for item in case.get("evidence", [])],
                "bm25_score": round(lexical_score, 4),
                "vector_score": round(vector_score, 4),
                "score": round(fused_score, 4),
            }
        )

    ranked = sorted(fused_hits, key=lambda hit: float(hit.get("score", 0.0)), reverse=True)
    return ranked[:top_k]