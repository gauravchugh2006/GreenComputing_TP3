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
You will reuse the TP2 data:

1. **`books.csv`**  
   - `Title`, `Description`, `Authors`, `Publisher`, `PublishedDate`, `Categories`, `RatingsCount`, …  
2. **`reviews.csv`**  
   - `Id`, `Title`, `Price`, `User_id`, `profileName`, `review/score`, `review/text`, `review/time`, … :contentReference[oaicite:3]{index=3}

👉 In the notebook: add a short cell that **loads and displays** the head of each CSV (`books.head()`, `reviews.head()`) so the user understands columns before processing.

---

## 4. Experimental Design
**Goal:** run **two identical pipelines** that differ **only** in file format.  
Pipelines:

1. **Load** `books` + `reviews`
2. **Clean** (missing values, normalize authors/categories)
3. **Join** on `Title`
4. **Compute metrics**:
   - avg rating per author
   - # reviews per publisher
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
**Purpose:** establish a reference in the *current* format.  
**What to code:**

```python
from codecarbon import EmissionsTracker
import pandas as pd

tracker = EmissionsTracker(project_name="csv_pipeline")
tracker.start()

books = pd.read_csv("data/books.csv")
reviews = pd.read_csv("data/reviews.csv")

df = (
    reviews.merge(books, on="Title", how="inner")
    # add cleaning here: dropna, normalize authors, etc.
)

df.to_csv("outputs/merged_books_reviews.csv", index=False)

emissions = tracker.stop()
print("CSV pipeline emissions (kg):", emissions)
````

**What to run:**

1. Run the **install/import** cell (CodeCarbon + pandas).
2. Run the **CSV pipeline** cell.
3. Check that it produced:

   * `outputs/merged_books_reviews.csv`
   * `emissions.csv` (default CodeCarbon log) → rename/export as `emissions_csv.csv` as per instructions. 

**What to understand:** this is the **baseline** — all later measurements are compared to this.

---

### Task 2 — Parquet Pipeline

**Purpose:** do **the same ETL** but in Parquet.

**Step A – convert raw CSV → Parquet** (one-off):

```python
books = pd.read_csv("data/books.csv")
reviews = pd.read_csv("data/reviews.csv")

books.to_parquet("data/books.parquet", compression="snappy")
reviews.to_parquet("data/reviews.parquet", compression="snappy")
```

**Step B – run the ETL in Parquet with CodeCarbon:**

```python
from codecarbon import EmissionsTracker
import pandas as pd

tracker = EmissionsTracker(project_name="parquet_pipeline")
tracker.start()

books = pd.read_parquet("data/books.parquet")
reviews = pd.read_parquet("data/reviews.parquet")

df = (
    reviews.merge(books, on="Title", how="inner")
    .dropna(subset=["review/score", "Authors"])
)

df.to_parquet(
    "outputs/merged_books_reviews.parquet",
    compression="snappy"
)

emissions = tracker.stop()
print("Parquet pipeline emissions (kg):", emissions)
```

Then **export**/rename CodeCarbon log to `emissions_parquet.csv`. 

**What to understand:** if Parquet is smaller and faster to read/write, it should **also** show lower CO₂ for the same work.

---

### Task 3 — Comparison and Analysis

Now read both emission logs and build a **summary table** like the one in the PDF:

| Step       | Format  | Duration (s) | Energy (kWh) | CO₂ (kg) | Output Size (MB) |
| ---------- | ------- | ------------ | ------------ | -------- | ---------------- |
| Load books | CSV     | ...          | ...          | ...      | ...              |
| Load books | Parquet | ...          | ...          | ...      | ...              |
| Save merge | CSV     | ...          | ...          | ...      | ...              |
| Save merge | Parquet | ...          | ...          | ...      | ...              |

Then create **2 bar charts**:

1. **Runtime vs format**
2. **CO₂ vs format**

In notebook code (example):

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("analysis/format_comparison.csv")

df_runtime = df.pivot(index="Step", columns="Format", values="Duration_s")
df_runtime.plot(kind="bar", title="Runtime by format")

plt.show()

df_co2 = df.pivot(index="Step", columns="Format", values="CO2_kg")
df_co2.plot(kind="bar", title="CO₂ emissions by format")

plt.show()
```

**What to discuss** (put this in a markdown cell called `### Discussion`):

* Which format is **faster**?
* Which one **emits less CO₂**?
* How much **storage** is saved with Parquet?
* Does **compression always** reduce emissions, or only when I/O dominates? 

---

## 6. Task 4 — Eco-Design Experiment

The brief says: **pick ONE optimization**, re-run, compare before/after. Options:

* filter out unused columns **before** saving
* change compression (gzip, brotli, snappy)
* reduce dataset size (sample 50%)
* cache intermediates (if PySpark) 

A simple option for pandas/Parquet:

```python
keep_cols = ["Title", "Authors", "Publisher", "review/score", "review/text"]

tracker = EmissionsTracker(project_name="parquet_filtered")
tracker.start()

df_filtered = df[keep_cols]
df_filtered.to_parquet("outputs/merged_filtered.parquet", compression="snappy")

emissions_filtered = tracker.stop()
print("Filtered parquet emissions (kg):", emissions_filtered)
```

👉 Then add a markdown cell: **“Before vs After optimization”** with a small 4–5 line explanation:

* we removed columns
* file became smaller
* write time dropped
* CO₂ dropped accordingly
* note: if CPU compression ↑ a lot, benefit may shrink

---

## 7. Deliverables

As per the brief, the final repo/notebook should contain: 

1. `tp_codecarbon_parquet.ipynb` (your main notebook, structured exactly as above)
2. `emissions_csv.csv`
3. `emissions_parquet.csv`
4. **1 figure** comparing runtime and emissions (can be 2 matplotlib figures)
5. **A 10-line reflection** on trade-offs (readability, performance, sustainability)

👉 Add a markdown section in the notebook called **“Reflection (10 lines)”** and write it there so the grader finds it quickly.

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

   * `data/books.csv`
   * `data/reviews.csv`

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
   ```

5. **Launch**:

   ```bash
   jupyter notebook
   ```

6. Open `tp_codecarbon_parquet.ipynb` and run cells from top to bottom:

   * first the CSV baseline
   * then the Parquet pipeline
   * then the comparison
   * then the eco-design experiment

---

## Project Structure (suggested)

```text
.
├── data/
│   ├── books.csv
│   ├── reviews.csv
├── outputs/
│   ├── merged_books_reviews.csv
│   ├── merged_books_reviews.parquet
│   ├── merged_filtered.parquet
├── analysis/
│   └── format_comparison.csv
├── emissions_csv.csv
├── emissions_parquet.csv
├── tp_codecarbon_parquet.ipynb
└── README.md   ← (this file)
```

---

## Notes for the Grader / Reviewer

* Each notebook section maps **1:1** to the PDF sections 1–8.
* All CodeCarbon runs should have a **project_name** clearly indicating the scenario (`csv_pipeline`, `parquet_pipeline`, `parquet_filtered`).
* All plots must label axes: **x = step/format**, **y = time (s) or CO₂ (kg)**.

---

```
```
