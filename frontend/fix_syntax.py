import sys


def fix_file(filename, target):
    with open(filename, "r") as f:
        content = f.read()

    if target in content:
        content = content.replace(target, target + "\n    )")
        with open(filename, "w") as f:
            f.write(content)
        print(f"Fixed {filename}")
    else:
        print(f"Target not found in {filename}")


fix_file(
    "c:/Users/rahul/Documents/Aurora/frontend/frontend/components/sidebar.py",
    'class_name="w-[250px] border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-[#0b1120] flex flex-col shrink-0 z-10 h-full",\n    )',
)

fix_file(
    "c:/Users/rahul/Documents/Aurora/frontend/frontend/components/datagrid.py",
    'class_name="flex-1 overflow-hidden flex flex-col min-h-0 bg-white dark:bg-[#0f172a]",\n    )',
)
