from typing import Tuple, Dict, Any


class SQLGenerationError(Exception):
    """Raised when the query builder encounters an invalid or unsafe filter state."""

    def __init__(self, message: str, context: Any = None):
        if context:
            super().__init__(f"{message} (Context: {context})")
        else:
            super().__init__(message)
        self.context = context


class ParamGenerator:
    """Encapsulates parameter naming and value mapping to prevent collisions."""

    def __init__(self, start_counter: int = 1):
        self.counter = start_counter
        self.params: Dict[str, Any] = {}

    def get_next(self, prefix: str = "p") -> Tuple[str, str]:
        """Generates a new unique parameter name and placeholder."""
        name = f"{prefix}_{self.counter}"
        placeholder = f":{name}"
        self.counter += 1
        return name, placeholder

    def add(self, prefix: str, value: Any) -> Tuple[str, str]:
        """Helper to get a new parameter and immediately map its value."""
        name, placeholder = self.get_next(prefix)
        self.params[name] = value
        return name, placeholder

    def update(self, other_params: Dict[str, Any]):
        self.params.update(other_params)
