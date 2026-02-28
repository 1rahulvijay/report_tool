# Backward compatibility bridge for the split query_builder package
from .query_builder.base import SQLGenerationError, ParamGenerator
from .query_builder.service import QueryBuilderService

__all__ = ["SQLGenerationError", "ParamGenerator", "QueryBuilderService"]
