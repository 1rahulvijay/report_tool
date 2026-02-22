import sys
import py_compile

filename = "c:/Users/rahul/Documents/Aurora/frontend/frontend/components/sidebar.py"

with open(filename, "r") as f:
    content = f.read()

target = 'class_name="w-[250px] border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-[#0b1120] flex flex-col shrink-0 z-10 h-full",\n    )'

if target in content:
    content = content.replace(target, target + "\n    )")
    with open(filename, "w") as f:
        f.write(content)
    print(f"Fixed {filename}")
else:
    print(f"Target not found in {filename}")

try:
    py_compile.compile(filename, doraise=True)
    print("Syntax OK")
except Exception as e:
    print(f"Syntax Error: {e}")
