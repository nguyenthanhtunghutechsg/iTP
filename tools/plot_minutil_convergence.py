#!/usr/bin/env python3
"""Render minUtil convergence traces as a dependency-free SVG chart."""

from __future__ import annotations

import argparse
import csv
import math
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Callable, Iterable


REQUIRED_COLUMNS = {"sequence", "event", "elapsed_ms", "partition_index", "min_util"}
MILESTONES = (0.50, 0.75, 0.90, 0.95, 0.99, 1.00)
COLORS = ("#2563EB", "#EA580C", "#059669", "#7C3AED", "#DC2626", "#0891B2")


@dataclass
class TraceSummary:
    path: Path
    label: str
    start_elapsed_ms: int
    end_elapsed_ms: int
    max_sequence: int
    initial_min_util: int
    final_min_util: int
    update_count: int
    partition_count: int
    partition_elapsed_ms: list[int]
    partition_sequences: list[int]
    milestone_elapsed_ms: dict[float, int]
    milestone_sequences: dict[float, int]

    @property
    def duration_ms(self) -> int:
        return max(0, self.end_elapsed_ms - self.start_elapsed_ms)


def parse_int(row: dict[str, str], column: str, path: Path, row_number: int) -> int:
    try:
        return int(row[column])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"{path}: invalid {column!r} at CSV row {row_number}") from exc


def infer_label(path: Path) -> str:
    match = re.search(r"(?:^|_)wad([01])(?:_|$)", path.stem, re.IGNORECASE)
    if match:
        return "Work-aware SU" if match.group(1) == "1" else "Legacy order (WAD off)"
    name = path.stem.removesuffix("_threshold")
    return name[:64]


def scan_trace(path: Path, label: str) -> TraceSummary:
    start_elapsed = None
    end_elapsed = None
    max_sequence = -1
    initial_min_util = None
    final_min_util = None
    update_count = 0
    partition_elapsed: list[int] = []
    partition_sequences: list[int] = []
    previous_sequence = -1
    previous_min_util = -1

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS.difference(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path}: missing columns: {', '.join(sorted(missing))}")

        for row_number, row in enumerate(reader, start=2):
            sequence = parse_int(row, "sequence", path, row_number)
            elapsed = parse_int(row, "elapsed_ms", path, row_number)
            min_util = parse_int(row, "min_util", path, row_number)
            event = row["event"]

            if sequence <= previous_sequence:
                raise ValueError(f"{path}: sequence is not strictly increasing at CSV row {row_number}")
            if min_util < previous_min_util:
                raise ValueError(f"{path}: min_util decreases at CSV row {row_number}")
            previous_sequence = sequence
            previous_min_util = min_util

            if start_elapsed is None:
                start_elapsed = elapsed
                initial_min_util = min_util
            end_elapsed = elapsed
            max_sequence = sequence
            final_min_util = min_util

            if event == "MIN_UTIL_UPDATE":
                update_count += 1
            elif event == "PARTITION_START":
                partition_elapsed.append(elapsed)
                partition_sequences.append(sequence)

    if start_elapsed is None or end_elapsed is None or initial_min_util is None or final_min_util is None:
        raise ValueError(f"{path}: trace is empty")

    return TraceSummary(
        path=path,
        label=label,
        start_elapsed_ms=start_elapsed,
        end_elapsed_ms=end_elapsed,
        max_sequence=max_sequence,
        initial_min_util=initial_min_util,
        final_min_util=final_min_util,
        update_count=update_count,
        partition_count=len(partition_elapsed),
        partition_elapsed_ms=[value - start_elapsed for value in partition_elapsed],
        partition_sequences=partition_sequences,
        milestone_elapsed_ms={},
        milestone_sequences={},
    )


def downsample(
    summary: TraceSummary,
    common_max: int,
    point_count: int,
    x_value: Callable[[dict[str, str]], int],
    capture_milestones: bool = False,
) -> list[int]:
    values: list[int | None] = [None] * point_count
    values[0] = summary.initial_min_util
    denominator = max(1, common_max)

    with summary.path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            x = max(0, min(common_max, x_value(row)))
            min_util = int(row["min_util"])
            bucket = min(point_count - 1, (x * (point_count - 1)) // denominator)
            values[bucket] = min_util
            if capture_milestones:
                for fraction in MILESTONES:
                    if fraction not in summary.milestone_elapsed_ms and min_util >= summary.final_min_util * fraction:
                        summary.milestone_elapsed_ms[fraction] = int(row["elapsed_ms"]) - summary.start_elapsed_ms
                        summary.milestone_sequences[fraction] = int(row["sequence"])

    carried = summary.initial_min_util
    result: list[int] = []
    for value in values:
        if value is not None:
            carried = value
        result.append(carried)

    # Carry the final threshold after this run ends until the shared x-axis ends.
    return result


def fmt_int(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.1f}M".replace(".0M", "M")
    if absolute >= 1_000:
        return f"{value / 1_000:.1f}K".replace(".0K", "K")
    return f"{value:.0f}"


def fmt_duration(milliseconds: int) -> str:
    seconds = milliseconds / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.2f}m"


def step_path(values: list[int], x: float, y: float, width: float, height: float, y_max: int) -> str:
    if not values:
        return ""
    denominator = max(1, len(values) - 1)
    scale_y = max(1, y_max)

    def px(index: int) -> float:
        return x + width * index / denominator

    def py(value: int) -> float:
        return y + height * (1 - value / scale_y)

    parts = [f"M {px(0):.2f} {py(values[0]):.2f}"]
    previous = values[0]
    for index, value in enumerate(values[1:], start=1):
        if value != previous:
            parts.append(f"H {px(index):.2f} V {py(value):.2f}")
            previous = value
    parts.append(f"H {x + width:.2f}")
    return " ".join(parts)


def xml_text(x: float, y: float, content: str, css_class: str, anchor: str = "start") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" class="{css_class}" '
        f'text-anchor="{anchor}">{escape(content)}</text>'
    )


def latex_escape(value: str) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(character, character) for character in value)


def render_panel(
    summaries: list[TraceSummary],
    series: list[list[int]],
    panel_x: float,
    panel_y: float,
    panel_width: float,
    panel_height: float,
    title: str,
    x_title: str,
    common_max: int,
    y_max: int,
    x_formatter: Callable[[float], str],
    partition_values: Callable[[TraceSummary], Iterable[int]],
) -> str:
    left = panel_x + 84
    top = panel_y + 48
    width = panel_width - 112
    height = panel_height - 112
    bottom = top + height
    elements = [f'<g aria-label="{escape(title)}">', xml_text(panel_x, panel_y + 20, title, "panel-title")]

    for tick in range(6):
        fraction = tick / 5
        yy = bottom - height * fraction
        value = y_max * fraction
        elements.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{left + width}" y2="{yy:.1f}" class="grid"/>')
        elements.append(xml_text(left - 10, yy + 4, fmt_int(value), "tick", "end"))

    for tick in range(6):
        fraction = tick / 5
        xx = left + width * fraction
        value = common_max * fraction
        elements.append(f'<line x1="{xx:.1f}" y1="{top}" x2="{xx:.1f}" y2="{bottom}" class="grid vertical"/>')
        elements.append(xml_text(xx, bottom + 22, x_formatter(value), "tick", "middle"))

    elements.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" class="axis"/>')
    elements.append(f'<line x1="{left}" y1="{bottom}" x2="{left + width}" y2="{bottom}" class="axis"/>')
    elements.append(xml_text(left + width / 2, bottom + 50, x_title, "axis-title", "middle"))
    elements.append(
        f'<text x="{panel_x + 18}" y="{top + height / 2}" class="axis-title" text-anchor="middle" '
        f'transform="rotate(-90 {panel_x + 18} {top + height / 2})">minUtil</text>'
    )

    for index, summary in enumerate(summaries):
        color = COLORS[index % len(COLORS)]
        for value in partition_values(summary):
            if 0 <= value <= common_max:
                xx = left + width * value / max(1, common_max)
                elements.append(
                    f'<line x1="{xx:.2f}" y1="{bottom - 5 - index * 5}" x2="{xx:.2f}" '
                    f'y2="{bottom - 1 - index * 5}" stroke="{color}" stroke-opacity="0.28" stroke-width="1"/>'
                )
        path = step_path(series[index], left, top, width, height, y_max)
        elements.append(
            f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.4" '
            f'stroke-linejoin="round" vector-effect="non-scaling-stroke"/>'
        )

    elements.append("</g>")
    return "\n".join(elements)


def render_svg(
    summaries: list[TraceSummary],
    elapsed_series: list[list[int]],
    output: Path,
) -> None:
    canvas_width = 1200
    canvas_height = 790
    common_duration = max(summary.duration_ms for summary in summaries)
    raw_y_max = max(summary.final_min_util for summary in summaries)
    magnitude = 10 ** max(0, int(math.floor(math.log10(max(1, raw_y_max)))) - 1)
    y_max = int(math.ceil(raw_y_max / magnitude) * magnitude)
    plot_x = 142
    plot_y = 52
    plot_width = 985
    plot_height = 560
    plot_bottom = plot_y + plot_height

    elements = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{canvas_width}" height="{canvas_height}" viewBox="0 0 {canvas_width} {canvas_height}">',
        """<style>
            text { font-family: Arial, Helvetica, sans-serif; fill: #111111; }
            .axis-title { font-size: 23px; }
            .tick { font-size: 19px; }
            .legend { font-size: 20px; }
            .note { font-size: 14px; fill: #444444; }
            .caption { font-family: Georgia, 'Times New Roman', serif; font-size: 27px; }
            .frame { fill: none; stroke: #111111; stroke-width: 2; }
        </style>""",
        '<rect width="100%" height="100%" fill="#FFFFFF"/>',
        f'<rect x="{plot_x}" y="{plot_y}" width="{plot_width}" height="{plot_height}" class="frame"/>',
    ]

    for tick in range(6):
        fraction = tick / 5
        xx = plot_x + plot_width * fraction
        yy = plot_bottom - plot_height * fraction
        tick_length = 13
        elements.extend(
            [
                f'<line x1="{xx:.1f}" y1="{plot_y}" x2="{xx:.1f}" y2="{plot_y + tick_length}" stroke="#111" stroke-width="2"/>',
                f'<line x1="{xx:.1f}" y1="{plot_bottom}" x2="{xx:.1f}" y2="{plot_bottom - tick_length}" stroke="#111" stroke-width="2"/>',
                f'<line x1="{plot_x}" y1="{yy:.1f}" x2="{plot_x + tick_length}" y2="{yy:.1f}" stroke="#111" stroke-width="2"/>',
                f'<line x1="{plot_x + plot_width}" y1="{yy:.1f}" x2="{plot_x + plot_width - tick_length}" y2="{yy:.1f}" stroke="#111" stroke-width="2"/>',
                xml_text(xx, plot_bottom + 32, f"{common_duration * fraction / 1000:.0f}", "tick", "middle"),
                xml_text(plot_x - 16, yy + 7, f"{y_max * fraction:.1e}", "tick", "end"),
            ]
        )

    for index, (summary, values) in enumerate(zip(summaries, elapsed_series)):
        is_wad_off = re.search(r"(?:^|_)wad0(?:_|$)", summary.path.stem, re.IGNORECASE) is not None
        dash = ' stroke-dasharray="2 6" stroke-linecap="round"' if is_wad_off or (index % 2 == 0 and len(summaries) > 1) else ""
        path = step_path(values, plot_x, plot_y, plot_width, plot_height, y_max)
        elements.append(
            f'<path d="{path}" fill="none" stroke="#111111" stroke-width="2.4"{dash} '
            f'stroke-linejoin="round" vector-effect="non-scaling-stroke"/>'
        )
        finish_x = plot_x + plot_width * summary.duration_ms / max(1, common_duration)
        finish_y = plot_y + plot_height * (1 - summary.final_min_util / max(1, y_max))
        elements.append(
            f'<circle cx="{finish_x:.2f}" cy="{finish_y:.2f}" r="4.5" fill="#FFFFFF" '
            f'stroke="#111111" stroke-width="2"/>'
        )

    legend_x = plot_x + plot_width * 0.48
    legend_y = plot_y + plot_height * 0.68
    for index, summary in enumerate(summaries):
        yy = legend_y + index * 39
        is_wad_off = re.search(r"(?:^|_)wad0(?:_|$)", summary.path.stem, re.IGNORECASE) is not None
        dash = ' stroke-dasharray="2 6" stroke-linecap="round"' if is_wad_off or (index % 2 == 0 and len(summaries) > 1) else ""
        elements.append(f'<line x1="{legend_x}" y1="{yy}" x2="{legend_x + 92}" y2="{yy}" stroke="#111" stroke-width="2.4"{dash}/>')
        elements.append(xml_text(legend_x + 112, yy + 7, f"{summary.label} ({summary.duration_ms / 1000:.1f} s)", "legend"))

    elements.append(xml_text(plot_x + plot_width / 2, plot_bottom + 80, f"elapsed time (s), common horizon = {common_duration / 1000:.1f} s", "axis-title", "middle"))
    elements.append(
        f'<text x="42" y="{plot_y + plot_height / 2}" class="axis-title" text-anchor="middle" '
        f'transform="rotate(-90 42 {plot_y + plot_height / 2})">minimum utility threshold (minUtil)</text>'
    )
    elements.append(xml_text(plot_x + plot_width / 2, plot_bottom + 119, "Open circle = actual completion; the final minUtil is then carried forward.", "note", "middle"))
    elements.append(xml_text(plot_x + plot_width / 2, plot_bottom + 161, "The utility threshold", "caption", "middle"))

    elements.append("</svg>")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(elements), encoding="utf-8")


def render_tex(
    summaries: list[TraceSummary],
    elapsed_series: list[list[int]],
    output: Path,
) -> list[Path]:
    common_duration = max(summary.duration_ms for summary in summaries)
    common_seconds = common_duration / 1000
    raw_y_max = max(summary.final_min_util for summary in summaries)
    magnitude = 10 ** max(0, int(math.floor(math.log10(max(1, raw_y_max)))) - 1)
    y_max = int(math.ceil(raw_y_max / magnitude) * magnitude)

    data_paths: list[Path] = []
    for index, (summary, values) in enumerate(zip(summaries, elapsed_series), start=1):
        wad_match = re.search(r"(?:^|_)wad([01])(?:_|$)", summary.path.stem, re.IGNORECASE)
        series_name = f"wad{wad_match.group(1)}" if wad_match else f"series{index}"
        data_path = output.with_name(f"{output.stem}_{series_name}.csv")
        with data_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["elapsed_s", "min_util"])
            denominator = max(1, len(values) - 1)
            for point_index, min_util in enumerate(values):
                elapsed_seconds = common_seconds * point_index / denominator
                writer.writerow([f"{elapsed_seconds:.4f}", min_util])
        data_paths.append(data_path)

    lines = [
        r"\documentclass[tikz,border=3pt]{standalone}",
        r"\usepackage{pgfplots}",
        r"\pgfplotsset{compat=1.18}",
        r"\begin{document}",
        r"\begin{tikzpicture}",
        r"\begin{axis}[",
        r"  width=12.5cm,",
        r"  height=8.0cm,",
        r"  xmin=0,",
        f"  xmax={common_seconds:.3f},",
        r"  ymin=0,",
        f"  ymax={y_max},",
        r"  xlabel={Elapsed time (s)},",
        r"  ylabel={Minimum utility threshold ($\mathit{minUtil}$)},",
        r"  axis line style={black,semithick},",
        r"  tick align=inside,",
        r"  tick pos=both,",
        r"  scaled y ticks=base 10:6,",
        r"  legend style={draw=none,fill=none,font=\small},",
        r"  legend pos=south east,",
        r"  every axis plot/.append style={black,semithick,no marks},",
        r"  clip=false,",
        r"]",
    ]

    for index, (summary, data_path) in enumerate(zip(summaries, data_paths)):
        is_wad_off = re.search(r"(?:^|_)wad0(?:_|$)", summary.path.stem, re.IGNORECASE) is not None
        style = "densely dotted" if is_wad_off or (index % 2 == 0 and len(summaries) > 1) else "solid"
        lines.append(f"% Source trace: {summary.path.name}")
        lines.append(
            f"\\addplot+[const plot,{style}] table["
            f"x=elapsed_s,y=min_util,col sep=comma] {{{data_path.name}}};"
        )
        lines.append(f"\\addlegendentry{{{latex_escape(summary.label)}}}")

        # An open marker shows where real execution ended; the curve after it is carry-forward.
        lines.append(
            "\\addplot+[only marks,mark=o,mark size=2.2pt,forget plot] coordinates "
            f"{{({summary.duration_ms / 1000:.4f},{summary.final_min_util})}};"
        )

    lines.extend(
        [
            r"\end{axis}",
            r"\end{tikzpicture}",
            r"\end{document}",
        ]
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return data_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot minUtil convergence from threshold traces as SVG or standalone LaTeX PGFPlots.",
    )
    parser.add_argument("traces", nargs="+", type=Path, help="One or more *_threshold.csv files")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("result/minutil_convergence.tex"),
        help="Output .tex (PGFPlots, default) or .svg file",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Series label; repeat in the same order as trace files (otherwise inferred from wad0/wad1)",
    )
    parser.add_argument("--points", type=int, default=800, help="Horizontal samples per series (default: 800)")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.points < 100:
        raise SystemExit("--points must be at least 100")
    if args.label and len(args.label) != len(args.traces):
        raise SystemExit("Provide either no --label values or exactly one per trace")

    paths = [path.expanduser().resolve() for path in args.traces]
    labels = args.label or [infer_label(path) for path in paths]
    summaries = [scan_trace(path, label) for path, label in zip(paths, labels)]
    common_duration = max(summary.duration_ms for summary in summaries)
    elapsed_series = [
        downsample(
            summary,
            common_duration,
            args.points,
            lambda row, start=summary.start_elapsed_ms: int(row["elapsed_ms"]) - start,
            True,
        )
        for summary in summaries
    ]
    output = args.output.expanduser().resolve()
    generated_data_paths: list[Path] = []
    if output.suffix.lower() == ".tex":
        generated_data_paths = render_tex(summaries, elapsed_series, output)
    elif output.suffix.lower() == ".svg":
        render_svg(summaries, elapsed_series, output)
    else:
        raise SystemExit("Output extension must be .tex or .svg")
    print(f"Chart: {output}")
    for data_path in generated_data_paths:
        print(f"Data:  {data_path}")
    for summary in summaries:
        print(
            f"- {summary.label}: runtime={summary.duration_ms} ms, rows={summary.max_sequence + 1}, "
            f"updates={summary.update_count}, partitions={summary.partition_count}, "
            f"final minUtil={summary.final_min_util}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
