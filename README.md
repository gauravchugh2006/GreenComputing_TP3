# GreenComputing_TP3
````markdown
# TP3 – Measuring the Environmental Impact of File Formats (CSV vs Parquet)

This README explains how to **read**, **run**, and **understand** the Python notebook that implements the practical work described in the attached brief. It follows the same structure (steps 1 → 8) as the PDF so you can map notebook cells to the original instructions easily. :contentReference[oaicite:0]{index=0}

---

## Table of Contents
1. [Learning Objectives](#1-learning-objectives)
2. [Context](#2-context)
3. [Datasets Overview](#3-datasets-overview)
4. [Experimental Design](#4-experimental-design)
5. [Tasks (Notebook Workflow)](#5-tasks-notebook-workflow)
   - [Task 1 — CSV Baseline](#task-1--csv-baseline)
   - [Task 2 — Parquet Pipeline](#task-2--parquet-pipeline)
   - [Task 3 — Comparison-and-Analysis](#task-3--comparison-and-analysis)
6. [Task 4 — Eco-Design Experiment](#6-task-4--eco-design-experiment)
7. [Deliverables](#7-deliverables)
8. [Expected Outcomes](#8-expected-outcomes)
9. [How to Run the Notebook](#how-to-run-the-notebook)
10. [Project Structure (suggested)](#project-structure-suggested)
11. [Command Line Benchmark Script](#11-command-line-benchmark-script)

---

## 1. Learning Objectives
In this TP you will learn to:  
- See how **storage formats** (CSV vs Parquet) affect performance and energy.  
- **Instrument** data pipelines with **CodeCarbon** to measure runtime and CO₂.  
- **Compare** two runs of the *same* pipeline that differ **only** by file format.  
- **Explain** results in terms of *I/O*, *compression*, and *greener ETL choices*. :contentReference[oaicite:1]{index=1}

👉 In the notebook: add a **header markdown cell** called `# Objectives` and restate these 4 points so the reader knows what they will get.

---

## 2. Context
You work as a **Data Engineer** on a books & reviews processing flow. The current flow uses CSV for everything — it’s simple but not optimal. Your manager wants to know if **switching to Parquet (columnar + compressed)** makes the pipeline **faster and greener**.  
Your job: **replay the same pipeline twice** — once with CSV, once with Parquet — and **compare**. :contentReference[oaicite:2]{index=2}

👉 In the notebook: create a markdown cell `## Context` and explain in 3–4 lines what you’re benchmarking.

---

## 3. Datasets Overview
Two source files are required under `data/` before you run the notebook:

1. **`books_data.csv`** – bibliographic metadata with columns such as `Title`, `Description`, `Authors`, `Publisher`, `PublishedDate`, `Categories`, and `RatingsCount`.
2. **`Books_rating.csv`** – crowd-sourced reviews with `Id`, `Title`, `Price`, `User_id`, `profileName`, `review/score`, `review/text`, and `review/time`. :contentReference[oaicite:3]{index=3}

The notebook validates that both CSVs exist, previews a handful of rows, and creates synchronised Parquet copies so the CSV and Parquet pipelines analyse identical content.

---

## 4. Experimental Design
**Goal:** run **two identical pipelines** that differ **only** in file format.  
Pipelines:

1. **Load** `books` + `reviews`
2. **Clean** (missing values, normalize authors/categories)
3. **Join** on `Title`
4. **Compute metrics**:
   - avg rating per author
   - reviews per publisher
   - top 10 most-reviewed categories
5. **Text processing**: avg review length, most frequent keywords
6. **Save results** **in the same format** (CSV pipeline → CSV, Parquet pipeline → Parquet) :contentReference[oaicite:4]{index=4}

👉 In the notebook: represent this as a small markdown list **once**, then have two sections:
- `### Pipeline A – CSV`
- `### Pipeline B – Parquet`

Both must call the **same** functions where possible — only I/O changes.

---

## 5. Tasks (Notebook Workflow)

Your PDF defines 4 tasks, which correspond to the main notebook sections. Below is how to **code**, **run**, and **interpret** each of them. :contentReference[oaicite:5]{index=5}

### Task 1 — CSV Baseline
**Purpose:** establish the performance and sustainability baseline using the raw CSV assets.

The notebook provides a reusable `run_pipeline` helper that loads the CSVs, cleans and enriches the records, merges books with reviews, and computes the five requested analytics tasks:

1. Average rating per author.
2. Reviews per publisher.
3. Top 10 most-reviewed categories.
4. Average review length.
5. Most frequent keywords.

Running the Task 1 cell executes the CSV pipeline, records runtime plus CodeCarbon emissions, exports the merged dataset to `outputs/merged_books_reviews_csv.csv`, and saves a summary snapshot to `analysis/csv_pipeline_summary.csv`.

---

### Task 2 — Parquet Pipeline

**Purpose:** replay the identical ETL flow using Parquet inputs/outputs.

As part of the setup, the notebook refreshes `books_data.parquet` and `Books_rating.parquet` from the CSV sources (skipping the conversion when the Parquet files are already up-to-date). The Task 2 cell reuses `run_pipeline`, swapping in Parquet loaders and writers. The merged dataset is exported to `outputs/merged_books_reviews_parquet.parquet`, with pipeline metrics captured in `analysis/parquet_pipeline_summary.csv`.

Because only the storage format changes, differences in runtime, energy consumption, and CO₂ directly reflect the efficiency gains from Parquet.

---

### Task 3 — Comparison and Analysis

Running the comparison cell builds all required artefacts automatically:

1. `analysis/format_comparison.csv` — pipeline-level runtime, energy (kWh), and CO₂ (kg) for each format.
2. `analysis/format_task_comparison.csv` — per-task runtime/energy/emissions covering the five analytics tasks above.
3. `analysis/format_comparison.png` — a Matplotlib triptych contrasting runtime, energy, and CO₂.
4. `analysis/task_runtime_comparison.html` — a Plotly grouped bar chart comparing task runtimes by format.

These files are generated fresh on every run, so the repository no longer ships pre-seeded comparison data. Review the rendered tables and figures in the notebook output to explain which format is greener and which tasks dominate resource usage.

---

## 6. Task 4 — Eco-Design Experiment

The notebook demonstrates an eco-design tweak by writing a column-pruned Parquet dataset via `run_pipeline`. Only the columns needed for downstream analytics are persisted, which reduces I/O and emissions compared with the full Parquet export. Document the observed delta in the **“Before vs After optimization”** markdown cell.

---

## 7. Deliverables

As per the brief, the final repo/notebook should contain:

1. `tp_codecarbon_parquet.ipynb` (structured according to the PDF checklist).
2. Generated outputs under `outputs/` after execution:
   - `outputs/merged_books_reviews_csv.csv`
   - `outputs/merged_books_reviews_parquet.parquet`
   - `outputs/merged_books_reviews_parquet_filtered.parquet`
3. Generated analysis artefacts under `analysis/` after execution:
   - `analysis/csv_pipeline_summary.csv`
   - `analysis/parquet_pipeline_summary.csv`
   - `analysis/format_comparison.csv` and `analysis/format_comparison.png`
   - `analysis/format_task_comparison.csv` and `analysis/task_runtime_comparison.html`
4. **A reflection section (7–9 bullet points)** on trade-offs (readability, performance, sustainability)

👉 Add a markdown section in the notebook called **“Reflection (8 points)”** and write it there so the grader finds it quickly.

---

## 8. Expected Outcomes

When you run everything correctly, you should be able to **observe** (and therefore justify):

* Parquet files are **5–10× smaller** than CSV.
* Reading/writing Parquet is usually **faster** and **greener**.
* Picking the **right file format** is an actual **eco-design lever**: less storage, less I/O, less CO₂. 

👉 In the notebook: end with a final cell `### Conclusion` summarizing these 3 bullets.

---

## How to Run the Notebook

1. **Clone / download** this repo.

2. Put raw files in `data/`:

   * `data/books_data.csv`
   * `data/Books_rating.csv`

3. Create a virtual env (optional but recommended):

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # on Windows: .venv\Scripts\activate
   ```

4. **Install deps**:

   ```bash
   pip install -r requirements.txt
   ```

   Minimal requirements:

   ```text
   pandas
   pyarrow        # for parquet
   codecarbon
   matplotlib
   plotly
   ```

5. **Launch**:

   ```bash
   jupyter notebook
   ```

6. Open `tp_codecarbon_parquet.ipynb` and run cells from top to bottom. The notebook will validate the CSV inputs, refresh the Parquet copies, execute the CSV and Parquet pipelines, build comparison artefacts, and finally run the eco-design experiment.

---

## Project Structure (suggested)

```text
.
├── data/
│   ├── books_data.csv
│   ├── Books_rating.csv
│   ├── books_data.parquet
│   └── Books_rating.parquet
├── outputs/
│   ├── merged_books_reviews_csv.csv
│   ├── merged_books_reviews_parquet.parquet
│   └── merged_books_reviews_parquet_filtered.parquet
├── analysis/
│   ├── csv_pipeline_summary.csv
│   ├── parquet_pipeline_summary.csv
│   ├── format_comparison.csv
│   ├── format_comparison.png
│   ├── format_task_comparison.csv
│   └── task_runtime_comparison.html
├── tp_codecarbon_parquet.ipynb
└── README.md   ← (this file)
```

---

## 11. Command Line Benchmark Script

Prefer running the analysis without Jupyter? A standalone CLI is available at
`scripts/format_benchmark.py`. It mirrors the notebook pipeline, converting the
CSV datasets to Parquet, executing both versions with CodeCarbon, and exporting
the comparison artefacts.

```
python scripts/format_benchmark.py \
    --data-dir data \
    --outputs-dir outputs \
    --analysis-dir analysis
```

The script prints a JSON summary for each run and regenerates the same
CSV/Parquet reports and visualisations stored in the `analysis/` directory. Use
`--no-plots` when Matplotlib or Plotly are unavailable.

---

## Notes for the Grader / Reviewer

* Each notebook section maps **1:1** to the PDF sections 1–8.
* All CodeCarbon runs should have a **project_name** clearly indicating the scenario (`csv_pipeline`, `parquet_pipeline`, `parquet_filtered`).
* All plots must label axes: **x = step/format**, **y = time (s) or CO₂ (kg)**.

---

```
```
