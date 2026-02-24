import glob
import re

for f in glob.glob("tests/*.py"):
    with open(f, "r", encoding="utf-8") as file:
        content = file.read()

    # Replace QueryBuilderService(...) with QueryBuilderService()
    content = re.sub(r"QueryBuilderService\((.*?)\)", "QueryBuilderService()", content)

    with open(f, "w", encoding="utf-8") as file:
        file.write(content)
