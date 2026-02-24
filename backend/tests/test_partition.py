import sys
import os

# Add backend directory to sys.path so 'app.xyz' imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from app.db.factory import get_database_adapter


def main():
    try:
        db = get_database_adapter()
        res = db.get_partition_values("EMPLOYEE_ROSTER", "AS_OF_MONTH_SK")
        print(f"SUCCESS: {res}")
    except Exception as e:
        print(f"FAILED: {e}")


if __name__ == "__main__":
    main()
