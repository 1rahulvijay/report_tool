import sys
import py_compile

filename = "c:/Users/rahul/Documents/Aurora/frontend/frontend/components/datagrid.py"

with open(filename, "r") as f:
    content = f.read()

# Replace the mismatched block at the end
old_block = """            class_name="flex-1 overflow-auto custom-scrollbar px-8",
        ),
        # Pagination Footer (Hidden in Virtual Mode)"""

new_block = """            ),
            class_name="flex-1 overflow-auto custom-scrollbar px-8",
        ),
        # Pagination Footer (Hidden in Virtual Mode)"""

content = content.replace(old_block, new_block)

# Remove the extra ) at the end of the file introduced
content = content.replace(
    'min-h-0 bg-white dark:bg-[#0f172a]",\n    )\n\n    )',
    'min-h-0 bg-white dark:bg-[#0f172a]",\n    )',
)


with open(filename, "w") as f:
    f.write(content)

try:
    py_compile.compile(filename, doraise=True)
    print("Syntax OK")
except Exception as e:
    print(f"Syntax Error: {e}")
