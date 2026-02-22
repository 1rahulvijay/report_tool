import os
import re


def main():
    os.chdir(r"c:\Users\rahul\OneDrive\Documents\Aurora\frontend\frontend")
    with open("state.py", "r", encoding="utf-8") as f:
        content = f.read()

    classes = [
        "BaseState",
        "ColumnState",
        "FilterState",
        "JoinState",
        "AggregationState",
        "AppState",
    ]

    imports = """import reflex as rx
import httpx
from typing import List, Dict, Any, Optional
import os

# The base URL where our FastAPI backend is running
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080/api/v1")
"""
    blocks = {}

    for i, cls in enumerate(classes):
        next_cls = classes[i + 1] if i + 1 < len(classes) else None

        # Regex to match from this class definition to the next or end of file
        if next_cls:
            pattern = rf"(class {cls}\(.*?\)[\s\S]*?)(?=\nclass {next_cls}\()"
        else:
            pattern = rf"(class {cls}\(.*?\)[\s\S]*)"

        match = re.search(pattern, content)
        if match:
            blocks[cls] = match.group(1).strip() + "\n"
        else:
            print(f"Could not find {cls}")

    os.makedirs("state_modules", exist_ok=True)
    with open("state_modules/__init__.py", "w") as f:
        f.write("")

    with open("state_modules/base.py", "w", encoding="utf-8") as f:
        f.write(imports + "\n\n" + blocks["BaseState"])

    with open("state_modules/column.py", "w", encoding="utf-8") as f:
        f.write(imports + "\nfrom .base import BaseState\n\n" + blocks["ColumnState"])

    with open("state_modules/filter.py", "w", encoding="utf-8") as f:
        f.write(
            imports + "\nfrom .column import ColumnState\n\n" + blocks["FilterState"]
        )

    with open("state_modules/join.py", "w", encoding="utf-8") as f:
        f.write(imports + "\nfrom .filter import FilterState\n\n" + blocks["JoinState"])

    with open("state_modules/aggregation.py", "w", encoding="utf-8") as f:
        f.write(
            imports + "\nfrom .join import JoinState\n\n" + blocks["AggregationState"]
        )

    with open("state.py", "w", encoding="utf-8") as f:
        f.write(
            imports
            + "\nfrom .state_modules.aggregation import AggregationState\n\n"
            + blocks["AppState"]
        )

    print("DONE")


if __name__ == "__main__":
    main()
