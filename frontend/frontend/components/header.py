import reflex as rx
from frontend.config import COLORS, UI_CONFIG


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
            rx.hstack(
                rx.foreach(
                    UI_CONFIG["ROUTING_LINKS"],
                    lambda link: rx.link(
                        rx.hstack(
                            rx.icon(tag=link["icon"], size=16),
                            rx.text(link["name"], size="2", font_weight="medium"),
                            spacing="2",
                            align="center",
                        ),
                        href=link["path"],
                        class_name="text-slate-400 hover:text-white transition-colors px-3 py-1.5 rounded-md hover:bg-white/5",
                    ),
                ),
                spacing="2",
                align="center",
                class_name="ml-8",
            ),
            class_name="flex items-center gap-4",
        ),
        class_name=f"h-[{UI_CONFIG['NAVBAR_HEIGHT']}] bg-[{COLORS['header_bg']}] text-white flex items-center justify-between px-6 border-b border-slate-800/50 shrink-0 z-20 w-full",
    )
