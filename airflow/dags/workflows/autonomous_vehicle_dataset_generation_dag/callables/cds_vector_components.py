# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDS-side input routers for precomputed Cosmos-Embed1-224p vectors only."""

import math
from typing import Any, Dict, List

from haystack import Document, component
from src.haystack.serializer import SerializerMixin
from src.visual_search.common.models import EmbeddingQuery, QueryType
from src.visual_search.exceptions import InputValidationError


def validate_vector(vector: List[float]) -> None:
    if len(vector) != 256 or not all(
        isinstance(value, (int, float)) and math.isfinite(value) for value in vector
    ):
        raise InputValidationError("Expected a finite 256-dimensional Curator embedding")


@component
class EmbeddingDocuments(SerializerMixin):
    @component.output_types(embedded=List[Document])
    def run(self, documents: List[Document]) -> Dict[str, Any]:
        for document in documents:
            if document.embedding is None or document.content or document.blob is not None:
                raise InputValidationError(
                    "This pipeline accepts precomputed embedding documents only"
                )
            validate_vector(document.embedding)
        return {"embedded": documents}


@component
class EmbeddingQueries(SerializerMixin):
    @component.output_types(embeddings=List[List[float]])
    def run(self, query: QueryType) -> Dict[str, Any]:
        queries = query if isinstance(query, (list, tuple)) else [query]
        if not queries or any(not isinstance(item, EmbeddingQuery) for item in queries):
            raise InputValidationError("This pipeline accepts precomputed embedding queries only")
        vectors = [list(item.embedding) for item in queries]
        for vector in vectors:
            validate_vector(vector)
        return {"embeddings": vectors}
