"""Utilities to benchmark CSV vs Parquet pipelines with CodeCarbon.

This module mirrors the notebook logic so that the analysis can be executed
from the command line.  It expects the two CSV inputs present in the TP data
folder and will generate the Parquet copies automatically when required.
"""
from __future__ import annotations

import argparse
import json
import math
import time
import tracemalloc
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:  # Optional dependencies
    import matplotlib.pyplot as plt  # type: ignore
except Exception:  # pragma: no cover - plotting is optional at runtime
    plt = None  # type: ignore

try:
    import plotly.express as px  # type: ignore
except Exception:  # pragma: no cover - plotting is optional at runtime
    px = None  # type: ignore

try:
    from codecarbon import EmissionsTracker
except Exception:  # pragma: no cover - CodeCarbon may be unavailable
    EmissionsTracker = None  # type: ignore


PREFERRED_POWER_KW = 0.15
EMISSIONS_FACTOR_KG_PER_KWH = 0.4


@dataclass
class TaskBreakdown:
    task_id: str
    task_label: str
    duration_s: float
    energy_kwh: float
    emissions_kg: float
    output_rows: int
    memory_peak_mb: float


@dataclass
class PipelineResult:
    format: str
    runtime_s: float
    energy_kwh: float
    emissions_kg: float
    row_count: int
    output_path: str
    output_size_bytes: int
    task_breakdown: List[TaskBreakdown]
    error: Optional[str] = None

    def to_summary_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["task_breakdown"] = [asdict(task) for task in self.task_breakdown]
        return payload

    def compact_dict(self) -> Dict[str, object]:
        summary = asdict(self)
        summary.pop("task_breakdown")
        return summary


def estimate_energy_and_emissions(duration_s: float) -> Tuple[float, float]:
    """Return an estimated energy usage (kWh) and emissions (kg CO₂)."""
    energy_kwh = (duration_s * PREFERRED_POWER_KW) / 3600.0
    emissions_kg = energy_kwh * EMISSIONS_FACTOR_KG_PER_KWH
    return energy_kwh, emissions_kg


def _series_with_default(df: pd.DataFrame, column: str, default) -> pd.Series:
    """Return a Series for ``column`` ensuring vector semantics."""

    if column in df:
        series = df[column]
        if isinstance(series, pd.Series):
            return series
        # Column may be a scalar-like object; broadcast to the frame length.
        return pd.Series([series] * len(df), index=df.index)
    if len(df.index) == 0:
        return pd.Series(dtype="object")
    return pd.Series([default] * len(df), index=df.index)


def clean_books(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the books dataset in place and return it."""

    authors = _series_with_default(df, "Authors", "Unknown").fillna("Unknown").astype(str)
    df["Authors"] = authors.str.title()

    df["Publisher"] = _series_with_default(df, "Publisher", "Unknown").fillna("Unknown").astype(str)
    df["Categories"] = _series_with_default(df, "Categories", "misc").fillna("misc").astype(str)

    df["PublishedDate"] = pd.to_datetime(df.get("PublishedDate"), errors="coerce")
    df["RatingsCount"] = pd.to_numeric(_series_with_default(df, "RatingsCount", 0), errors="coerce").fillna(0).astype(int)
    df["AverageRating"] = pd.to_numeric(df.get("AverageRating"), errors="coerce")
    return df


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the reviews dataset in place and return it."""

    df.rename(columns={"profileName": "ProfileName"}, inplace=True)
    df["review/text"] = _series_with_default(df, "review/text", "").fillna("").astype(str)

    review_scores = pd.to_numeric(_series_with_default(df, "review/score", math.nan), errors="coerce")
    mean_score = review_scores.mean() if not review_scores.dropna().empty else 0.0
    df["review/score"] = review_scores.fillna(mean_score)

    df["review/time"] = pd.to_datetime(df.get("review/time"), unit="s", errors="coerce")
    return df


def enrich_features(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    enriched["review_length"] = enriched["review/text"].astype(str).str.split().map(len)
    enriched["Categories"] = _series_with_default(enriched, "Categories", "misc").fillna("misc")
    enriched["CategoriesList"] = (
        enriched["Categories"].astype(str).str.split("|").apply(lambda values: [v.strip().lower() for v in values if v])
    )
    return enriched


def _compute_top_keywords(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Return the ``top_n`` most common keywords without loading the full corpus."""

    counter: Counter[str] = Counter()
    review_series = _series_with_default(df, "review/text", "")

    for review in review_series.astype(str):
        if not review:
            continue
        counter.update(word for word in review.lower().split())

    most_common = counter.most_common(top_n)
    if not most_common:
        return pd.DataFrame(columns=["keyword", "occurrences"])

    return pd.DataFrame(most_common, columns=["keyword", "occurrences"])


TASK_REGISTRY: List[Tuple[str, str, Callable[[pd.DataFrame], pd.DataFrame]]] = [
    (
        "avg_rating_per_author",
        "Average rating per author",
        lambda df: df.groupby("Authors")["review/score"].mean().reset_index().rename(
            columns={"review/score": "average_rating"}
        ).sort_values("average_rating", ascending=False),
    ),
    (
        "reviews_per_publisher",
        "Reviews per publisher",
        lambda df: df.groupby("Publisher")["Id"].count().reset_index().rename(
            columns={"Id": "review_count"}
        ).sort_values("review_count", ascending=False),
    ),
    (
        "top_categories",
        "Top 10 most-reviewed categories",
        lambda df: df.explode("CategoriesList").groupby("CategoriesList")["Id"].count().reset_index().rename(
            columns={"CategoriesList": "Category", "Id": "review_count"}
        ).sort_values("review_count", ascending=False).head(10),
    ),
    (
        "avg_review_length",
        "Average review length",
        lambda df: pd.DataFrame(
            [
                {
                    "metric": "average_review_length",
                    "value": df["review_length"].mean(),
                }
            ]
        ),
    ),
    (
        "top_keywords",
        "Most frequent review keywords",
        lambda df: _compute_top_keywords(df, top_n=20),
    ),
]


def compute_metrics(df: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], List[TaskBreakdown]]:
    metrics: Dict[str, pd.DataFrame] = {}
    breakdown: List[TaskBreakdown] = []
    for task_id, task_label, task_fn in TASK_REGISTRY:
        tracemalloc.start()
        task_start = time.perf_counter()
        try:
            frame = task_fn(df)
        finally:
            duration = time.perf_counter() - task_start
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        memory_peak_mb = peak_bytes / (1024 ** 2)
        energy_kwh, emissions_kg = estimate_energy_and_emissions(duration)
        metrics[task_id] = frame
        breakdown.append(
            TaskBreakdown(
                task_id=task_id,
                task_label=task_label,
                duration_s=duration,
                energy_kwh=energy_kwh,
                emissions_kg=emissions_kg,
                output_rows=int(len(frame)),
                memory_peak_mb=memory_peak_mb,
            )
        )
    return metrics, breakdown


def _create_tracker(project_name: str, analysis_dir: Path):
    emissions_dir = analysis_dir / "emissions"
    emissions_dir.mkdir(parents=True, exist_ok=True)
    output_file = f"{project_name}_emissions.jsonl"

    if EmissionsTracker is not None:
        try:
            return EmissionsTracker(
                project_name=project_name,
                output_dir=str(emissions_dir),
                output_file=output_file,
            )
        except Exception as tracker_error:  # pragma: no cover - fallback path
            print(f"Falling back to lightweight tracker because CodeCarbon failed: {tracker_error}")

    class FallbackTracker:
        def __init__(self, project_name: str, target_dir: Path, file_name: str) -> None:
            self.project_name = project_name
            self.target_dir = target_dir
            self.file_name = file_name
            self._start: Optional[float] = None

        def start(self) -> float:
            self._start = time.perf_counter()
            return self._start

        def stop(self) -> float:
            end = time.perf_counter()
            duration = end - (self._start or end)
            emissions = duration * 0.00012
            self._persist(
                {
                    "project_name": self.project_name,
                    "duration_s": duration,
                    "emissions_kg": emissions,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
            return emissions

        def _persist(self, payload: Dict[str, object]) -> None:
            try:
                self.target_dir.mkdir(parents=True, exist_ok=True)
                with (self.target_dir / self.file_name).open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload) + "\n")
            except Exception as persist_error:  # pragma: no cover - informational
                print(f"Could not persist fallback emissions data: {persist_error}")

    return FallbackTracker(project_name, emissions_dir, output_file)


def persist_metrics(
    format_name: str,
    df: pd.DataFrame,
    metrics: Dict[str, pd.DataFrame],
    output_path: Path,
    writer: Callable[[pd.DataFrame, Path], None],
    analysis_dir: Path,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    writer(df, output_path)
    try:
        output_size = output_path.stat().st_size
    except FileNotFoundError:
        output_size = 0

    prefix = f"{format_name}_{output_path.stem}"
    for name, frame in metrics.items():
        target = analysis_dir / f"{prefix}_{name}.csv"
        frame.to_csv(target, index=False)

    return output_size


def run_pipeline(
    format_name: str,
    loader: Callable[[], Tuple[pd.DataFrame, pd.DataFrame]],
    writer: Callable[[pd.DataFrame, Path], None],
    output_name: str,
    project_name: str,
    analysis_dir: Path,
    outputs_dir: Path,
) -> PipelineResult:
    tracker = _create_tracker(project_name, analysis_dir)
    start = time.perf_counter()
    emissions_from_tracker = math.nan
    error: Optional[str] = None
    merged_df: Optional[pd.DataFrame] = None
    output_size_bytes = 0

    try:
        tracker.start()
        books_df, reviews_df = loader()
        books_df = clean_books(books_df)
        reviews_df = clean_reviews(reviews_df)
        merged_df = enrich_features(
            reviews_df.merge(books_df, on="Title", how="inner", suffixes=("_review", "_book"))
        )
        metrics, task_breakdown = compute_metrics(merged_df)
        output_size_bytes = persist_metrics(
            format_name, merged_df, metrics, outputs_dir / output_name, writer, analysis_dir
        )
    except Exception as pipeline_error:
        error = str(pipeline_error)
        task_breakdown = []
        print(f"[{format_name}] Pipeline encountered an issue: {pipeline_error}")
    finally:
        duration = time.perf_counter() - start
        try:
            emissions_from_tracker = tracker.stop()
        except Exception as tracker_error:  # pragma: no cover - fallback path
            print(f"[{format_name}] Unable to obtain emissions from tracker: {tracker_error}")
        energy_kwh, estimated_emissions = estimate_energy_and_emissions(duration)
        emissions_kg = (
            emissions_from_tracker
            if isinstance(emissions_from_tracker, (int, float)) and not math.isnan(emissions_from_tracker)
            else estimated_emissions
        )
        if merged_df is None:
            output_size_bytes = 0

    return PipelineResult(
        format=format_name,
        runtime_s=duration,
        energy_kwh=energy_kwh,
        emissions_kg=emissions_kg,
        row_count=int(0 if merged_df is None else len(merged_df)),
        output_path=str(outputs_dir / output_name),
        output_size_bytes=int(output_size_bytes),
        task_breakdown=task_breakdown,
        error=error,
    )


def _load_csv(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return pd.read_csv(data_dir / "books_data.csv"), pd.read_csv(data_dir / "Books_rating.csv")


def _load_parquet(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return pd.read_parquet(data_dir / "books_data.parquet"), pd.read_parquet(data_dir / "Books_rating.parquet")


def _load_parquet_gzip(data_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return pd.read_parquet(data_dir / "books_data_gzip.parquet"), pd.read_parquet(
        data_dir / "Books_rating_gzip.parquet"
    )


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    try:
        df.to_parquet(path, index=False, compression="snappy")
    except Exception:  # pragma: no cover - fallback when snappy unavailable
        df.to_parquet(path, index=False)


def _write_filtered_parquet(df: pd.DataFrame, path: Path) -> None:
    important_columns = ["Id", "Title", "review/score", "review/text", "review_length", "Authors", "Categories"]
    filtered = df[important_columns]
    try:
        filtered.to_parquet(path, index=False, compression="snappy")
    except Exception:  # pragma: no cover
        filtered.to_parquet(path, index=False)


def _write_parquet_gzip(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, index=False, compression="gzip")


def _refresh_parquet_copies(data_dir: Path) -> None:
    refresh_specs = [
        ("books_data.csv", "books_data.parquet", clean_books),
        ("Books_rating.csv", "Books_rating.parquet", clean_reviews),
    ]
    for source_name, target_name, cleaner in refresh_specs:
        source = data_dir / source_name
        target_snappy = data_dir / target_name
        target_gzip = data_dir / f"{Path(target_name).stem}_gzip.parquet"

        needs_refresh = True
        if target_snappy.exists() and target_gzip.exists():
            try:
                source_mtime = source.stat().st_mtime
                needs_refresh = (
                    source_mtime > target_snappy.stat().st_mtime
                    or source_mtime > target_gzip.stat().st_mtime
                )
            except OSError:
                needs_refresh = True
            else:
                if not needs_refresh:
                    continue

        df_full = pd.read_csv(source)
        cleaner(df_full)
        df_full.to_csv(source, index=False)
        try:
            df_full.to_parquet(target_snappy, index=False, compression="snappy")
        except Exception:
            df_full.to_parquet(target_snappy, index=False)
        df_full.to_parquet(target_gzip, index=False, compression="gzip")


def _export_summary(
    results: Iterable[PipelineResult],
    analysis_dir: Path,
    generate_plots: bool,
) -> None:
    summary_records: List[Dict[str, object]] = []
    task_rows: List[Dict[str, object]] = []
    for result in results:
        summary_records.append(result.compact_dict())
        for task in result.task_breakdown:
            row = asdict(task)
            row["format"] = result.format
            task_rows.append(row)

    summary_df = pd.DataFrame(summary_records)
    summary_path = analysis_dir / "format_comparison.csv"
    summary_df.to_csv(summary_path, index=False)

    if generate_plots and plt is not None and not summary_df.empty:
        figure_path = analysis_dir / "format_comparison.png"
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        chart_specs = [
            ("runtime_s", "Runtime (s)", "Runtime by format", "#1f77b4"),
            ("energy_kwh", "Energy (kWh)", "Energy consumption by format", "#ff7f0e"),
            ("emissions_kg", "Emissions (kg CO₂)", "Carbon emissions by format", "#2ca02c"),
        ]
        for ax, (column, ylabel, title, color) in zip(axes, chart_specs):
            summary_df.plot.bar(x="format", y=column, ax=ax, color=color, legend=False)
            ax.set_ylabel(ylabel)
            ax.set_title(title)
            ax.set_xlabel("File format")
        fig.tight_layout()
        fig.savefig(figure_path, dpi=150)
        plt.close(fig)

    task_summary_df = pd.DataFrame(task_rows)
    task_path = analysis_dir / "format_task_comparison.csv"
    task_summary_df.to_csv(task_path, index=False)

    if generate_plots and px is not None and not task_summary_df.empty:
        plotly_path = analysis_dir / "task_runtime_comparison.html"
        plotly_fig = px.bar(
            task_summary_df,
            x="task_label",
            y="duration_s",
            color="format",
            barmode="group",
            title="Runtime by task and file format",
            labels={"task_label": "Task", "duration_s": "Runtime (s)", "format": "Format"},
        )
        plotly_fig.write_html(plotly_path)


def run_benchmark(data_dir: Path, outputs_dir: Path, analysis_dir: Path, generate_plots: bool = True) -> List[PipelineResult]:
    if not (data_dir / "books_data.csv").exists() or not (data_dir / "Books_rating.csv").exists():
        missing = [
            path.name
            for path in (data_dir / "books_data.csv", data_dir / "Books_rating.csv")
            if not path.exists()
        ]
        raise FileNotFoundError(
            "Missing required CSV files: " + ", ".join(missing)
        )

    outputs_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    _refresh_parquet_copies(data_dir)

    results: List[PipelineResult] = []
    results.append(
        run_pipeline(
            "csv",
            lambda: _load_csv(data_dir),
            _write_csv,
            "merged_books_reviews_csv.csv",
            "csv_pipeline",
            analysis_dir,
            outputs_dir,
        )
    )
    results.append(
        run_pipeline(
            "parquet",
            lambda: _load_parquet(data_dir),
            _write_parquet,
            "merged_books_reviews_parquet.parquet",
            "parquet_pipeline",
            analysis_dir,
            outputs_dir,
        )
    )
    results.append(
        run_pipeline(
            "parquet_gzip",
            lambda: _load_parquet_gzip(data_dir),
            _write_parquet_gzip,
            "merged_books_reviews_parquet_gzip.parquet",
            "parquet_gzip_pipeline",
            analysis_dir,
            outputs_dir,
        )
    )
    results.append(
        run_pipeline(
            "parquet_filtered",
            lambda: _load_parquet(data_dir),
            _write_filtered_parquet,
            "merged_books_reviews_parquet_filtered.parquet",
            "parquet_filtered_pipeline",
            analysis_dir,
            outputs_dir,
        )
    )

    _export_summary(results, analysis_dir, generate_plots)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark CSV vs Parquet pipelines with CodeCarbon metrics")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="Directory containing the input CSV files")
    parser.add_argument("--outputs-dir", type=Path, default=Path("outputs"), help="Destination for merged datasets")
    parser.add_argument("--analysis-dir", type=Path, default=Path("analysis"), help="Where to store summary artefacts")
    parser.add_argument("--no-plots", action="store_true", help="Disable generation of matplotlib/plotly visualisations")
    args = parser.parse_args()

    results = run_benchmark(
        data_dir=args.data_dir,
        outputs_dir=args.outputs_dir,
        analysis_dir=args.analysis_dir,
        generate_plots=not args.no_plots,
    )

    for result in results:
        print(json.dumps(result.to_summary_dict(), indent=2, default=float))


if __name__ == "__main__":
    main()
