# MonadSquishy: A Resilient Architecture for Heterogeneous Data Engineering

**MonadSquishy** is a functional, declarative data engineering framework designed to bridge the **"Silver Layer Gap"**—the critical architectural bottleneck where high-entropy, unstructured data (Bronze Layer) must be interpreted into strict, schema-enforced assets (Silver Layer).

In the architecture of contemporary data infrastructure, a significant divergence exists between data **ingestion** and **interpretation**. While tools like Airbyte and Fivetran have commoditized the transport of bytes, the logic required to validate and semanticize those bytes remains manual and fragile. Traditional imperative approaches—often characterized by nested "Arrow Code"—frequently resort to "Silent Failures" (coercion to `NaN`) to maintain uptime. This results in a permanent loss of context, rendering data issues indistinguishable from genuine missing values.

MonadSquishy mitigates this by utilizing the **Monad Pattern** and **Railway-Oriented Programming (ROP)**. This approach ensures strictly typed, resilient pipelines that prioritize data observability, preserving the provenance of every error without compromising the stability of the pipeline.

[notebook](./Draft15_Data_Quality_Dashboard.ipynb)🚀

![example reports](./visualization.png)


## 1. Theoretical Context

### The "Silver Layer Gap"

Modern Medallion Architectures (Lakehouse paradigms) organize data quality into progressions, yet they face a recurrent challenge at the transition point between raw storage and refined analytics:

  * **Bronze Layer (Raw):** Characterized by "Maximum Entropy" and absolute fidelity to the source system. This layer captures the state of the world, including its technical debt. It contains heterogeneous inputs such as mixed types (Integers co-mingled with Strings), ambiguous nulls (`"NULL"`, `"N/A"`, `"?"`), and unexpected formatting changes (e.g., `"$100.50"` vs `100`).
  * **Silver Layer (Refined):** Requires strict schema enforcement, type safety, and deduplication to serve as a trusted source for business intelligence.

**The Conflict:** Attempting to force Bronze data directly into Silver schemas without resilient middleware causes brittle pipelines that crash on edge cases. MonadSquishy acts as this resilient middleware, encapsulating state to guarantee that valid data enters the Silver layer while invalid data is safely diverted to a diagnostic Control Plane for remediation.

### The Legacy Problem: "Silent Failure"

Imperative scripts typically handle heterogeneity via deep nesting (Cyclomatic Complexity) and aggressive type coercion. A common anti-pattern observed in production is:

```python
# Legacy Anti-Pattern
df['price'] = pd.to_numeric(df['price'], errors='coerce') # All context is lost here
````

This approach is computationally expedient but functionally destructive. It treats genuine "missing data" (Row 5), upstream "string literals" (Row 4), and user-generated "garbage input" (Row 3) identically as `NaN`. Consequently, when downstream reports show zero revenue, engineers must engage in forensic investigations rather than systematic diagnosis. MonadSquishy preserves the distinct provenance of every failure, transforming debugging from a guessing game into a deterministic process.

-----

## 2\. Architectural Core

To address these challenges, MonadSquishy introduces a structured approach to state management at the cellular level.

### The Monad Container

The `Monad` class is the foundational data structure that wraps every individual data cell in a protective envelope. It abstracts away error propagation by encapsulating four distinct elements:

1.  **Value:** The mutable payload that evolves as it passes through transformers. Access is restricted to authorized binding functions.
2.  **Status:** A Finite State Machine (FSM) tracking the lifecycle of the data:
      * `pending`: Raw ingestion.
      * `valid`: Passed gatekeeper checks.
      * `dirty`: Failed validation or transformation.
      * `success`: Ready for the Silver Layer.
3.  **Logs:** A granular, append-only audit trail of every operation executed on the row. This provides row-level lineage, allowing engineers to reconstruct exactly *why* a specific value failed.
4.  **Stopped Flag:** The boolean switch mechanism that powers the Railway Topology, determining whether computation should proceed or halt.

### Railway-Oriented Programming (ROP)

The engine utilizes operator overloading (`|`) to implement a dual-track topology, eliminating the need for defensive `try/catch` blocks in business logic:

  * **🟢 Green Track (Happy Path):** Data passes validations and transformations sequentially. As long as functions return successfully, the data remains on this track.
  * **🔴 Red Track (Failure Path):** Upon a validation failure or chain exhaustion, the `Stopped Flag` is raised. The data effectively "teleports" to the terminal state, bypassing all remaining expensive downstream operations. This ensures that CPU cycles are never wasted attempting to parse data that has already been deemed invalid ("Zombie Execution").

### Atomic Units (SRP)

Logic is decomposed into atomic units adhering strictly to the **Single Responsibility Principle (SRP)**, enhancing testability and reusability:

  * **@validator (Fail-Fast):** Pure boolean gatekeepers. They do not mutate data; they only assert validity. If a constraint (e.g., `must_exist`) is violated, the process halts immediately to prevent garbage data from entering complex parsers.
  * **@transformer (Success-Fast):** Single-strategy conversion mechanisms. Polymorphic pipelines allow multiple transformers to be chained (e.g., `parse_iso`, then `parse_us`). The pipeline succeeds if *any* strategy resolves the input, allowing the system to negotiate with the data rather than forcing a single format.

-----

## 3\. The Squishy Engine

While the Monad handles atomic state, the **Squishy Engine** serves as the orchestration layer, managing execution across high-volume datasets using two key strategies to ensure scalability:

1.  **Parallel Execution:** The engine bypasses the Python Global Interpreter Lock (GIL) via `pandarallel`. It implements a scatter-gather architecture that partitions the DataFrame based on available CPU cores, serializes the Monadic logic, and executes transformations in true parallel processes.
2.  **Dual Observability (Tuple-Unpacking):** Standard map functions return a single value. The Squishy Engine's internal processor returns a composite data structure, segregating the **Signal** (The clean Silver Layer DataFrame for analysts) from the **Noise** (The Control Plane Logs for engineers). This ensures analytics tables remain pristine while engineers retain full visibility into data attrition.

-----

## 4\. Algorithmic Optimization (E-Score)

In cloud-native environments, computational latency correlates directly with cost. To minimize computational latency ($O(N)$), the engine calculates an **Efficiency Score (E-Score)** ($E = S/T$) for every transformer.

Following the **"Success-Fast" Heuristic** (conceptually analogous to Huffman Coding), pipelines are optimized by placing high-probability transformers at the start of the execution chain.

  * **Scenario:** If 90% of dates are ISO-8601, placing `parse_iso` first ensures 90% of rows are resolved in Step 1. Placing it last forces those rows to fail three other parsers before succeeding, tripling the computational load.

-----

## 5\. Observability & Visualization

The dashboard provides deep insight into the behavioral characteristics of the data, moving beyond binary pass/fail metrics to reveal systemic patterns.

### I. Data Quality Summary (Attrition Analysis)

**Purpose:** High-level health triage to quickly assess dataset viability.

  * **Logic:** Categorical separation into **Passed** (Safe for consumption), **Invalid** (Malformed), and **Missing** (Nulls).
  * **Ordering:** `Passed` $\to$ `Invalid` $\to$ `Missing`. This consistent ordering allows for rapid visual scanning of data health across dozens of columns.

### II. Operation Chain Flow (Throughput Assessment)

**Purpose:** Visualizes the topology of the "Railway" and identifies **"Survivorship Bias"** in downstream functions. A transformer might appear 100% effective simply because a strict upstream validator filtered out all difficult cases; this chart reveals that hidden funnel.

  * **Stack Hierarchy:**
    1.  **Skipped (Grey):** Data that succeeded early, bypassing cost (Efficiency Indicator).
    2.  **Validation Pass (Blue):** Data surviving gatekeeper checks (`@validator`).
    3.  **Transformer Pass (Green):** Data successfully resolved by a specific strategy (`@transformer`).
    4.  **Failed (Red):** Data rejected by a specific unit.
    <!-- end list -->
      * **Chain Exhausted:** The terminal state where data is valid (exists) but failed all configured transformation strategies. This indicates a gap in the pipeline's logic rather than a data error.

### III. AI Recommender (Latency Optimization)

**Purpose:** Data FinOps and **Drift Detection**.

  * **Mechanism:** Calculates the E-Score (0.0 - 1.0) for each function based on runtime performance.
  * **Action:** Suggests reordering the pipeline configuration to align with the statistical reality of the data distribution (e.g., moving `parse_iso` to Rank 1). If an API change causes a drop in `parse_iso` scores, this chart alerts engineers to the schema drift immediately.

-----

## 6\. Component Guide & Examples

### I. The Validator

A **Validator** acts as a "Fail-Fast" gatekeeper. It asserts specific conditions about the data.

**Example:**

```python
@validator
def must_exist(v):
    # Robust check for multiple "empty" states
    if pd.isna(v) or str(v).strip().lower() in ['null', 'n/a', '']:
        raise ValueError("Missing Value")
    return v
```

**Usage:** Place validators at the **start** of your pipeline list to filter out garbage data before expensive parsing begins.

### II. The Transformer

A **Transformer** acts as a "Success-Fast" parser. It attempts to convert the input into the target format.

**Example:**

```python
@transformer
def parse_currency(v):
    # Strategy for currency symbols
    if '$' in str(v):
        return float(str(v).replace('$', '').replace(',', ''))
    raise ValueError("No symbol found")
```

**Usage:** Chain multiple transformers to handle heterogeneous data. For example, `[parse_currency, parse_text_code, parse_plain_float]` covers `"$100"`, `"100 USD"`, and `"100.0"`.

### III. Efficiency Score (E-Score)

The E-Score quantifies the computational efficiency of a transformer.
$$ E = \frac{Successes (S)}{Total Rows (T)} $$

**Example:**

  * `parse_iso`: 8,500 successes / 10,000 rows = **0.85**
  * `parse_thai`: 0 successes / 10,000 rows = **0.00**

**Optimization:** Reorder your pipeline configuration based on these scores. High scores go first.

  * *Before:* `[parse_thai, parse_iso]` (Inefficient).
  * *After:* `[parse_iso, parse_thai]` (Efficient).

-----

## 7\. Usage

```python
# 1. Define Atomic Functions
# Validators act as gatekeepers
@validator
def must_exist(v): ...

# Transformers act as parsing strategies
@transformer
def parse_iso(v): ...

# 2. Configure Declarative Pipeline
# Define the intent: "Check if it exists, then try ISO, then try US format"
config = [
    {"target": "clean_date", "pipeline": [must_exist, parse_iso, parse_us]}
]

# 3. Execute Engine
engine = SquishyEngine(config, df)
engine.run()

# 4. Render Dashboard
show_visual_dashboard(engine, df, config)
