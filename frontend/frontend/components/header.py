import reflex as rx


def topnav() -> rx.Component:
    """The sticky top navigation bar."""
    return rx.box(
        rx.box(
            rx.hstack(
                rx.box(
                    rx.icon(tag="activity", size=18, class_name="text-white font-bold"),
                    class_name="w-7 h-7 bg-primary rounded shadow-lg shadow-blue-500/20 flex items-center justify-center",
                ),
                rx.box(
                    rx.text(
                        "Aurora",
                        class_name="font-bold text-xs tracking-[0.15em] text-white uppercase",
                    ),
                    class_name="flex flex-col leading-none",
                ),
                class_name="flex items-center gap-2.5",
            ),
            rx.box(class_name="h-5 w-px bg-slate-800 mx-2"),
            rx.box(
                rx.link(
                    "Data Studio",
                    href="#",
                    class_name="text-white font-bold border-b-2 border-primary py-3 hover:text-white transition-colors cursor-pointer",
                ),
                class_name="flex gap-8 text-[13px] font-medium text-slate-400",
            ),
            class_name="flex items-center gap-4",
        ),
        class_name="h-12 bg-[#020617] text-white flex items-center justify-between px-6 border-b border-slate-800/50 shrink-0 z-20 w-full",
    )
