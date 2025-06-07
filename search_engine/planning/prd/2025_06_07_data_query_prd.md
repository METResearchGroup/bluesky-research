# PRD: Bluesky Data Access App Demo (Streamlit)

## 1. 🧩 Objective

* **Goal:** Produce a clickable Streamlit demo of our curated Bluesky data‑access interface for social science researchers.
* **Measure of success:** Stakeholders can navigate end‑to‑end filter building, preview sample data, and simulate export flows—all within 5 minutes of first use.
* **Timeline:** Complete demo prototype by **2025‑06‑21** (two weeks)

## 2. 📌 Scope

### ✅ In‑Scope (Demo‑only)

* Streamlit front‑end UI for filter selection (no actual data fetching)
* Static sample datasets (5–10 rows each) representing posts, users, likes, retweets, comments
* Interactive UI components: accordions, chips, preview table, export button (simulated)
* Visualization Quick‑Look (mocked mini‑chart using sample data)
* Saved‑query templates panel (load frontend JSON)
* Simulated export flow: clicking “Export” downloads a sample CSV/Parquet

### ❌ Out‑of‑Scope (Deferred)

* Real backend/DB connections (DuckDB, S3, APIs)
* LLM integration or data transformations
* Network graph computations
* Authentication, authorization, user management
* Performance optimizations

## 3. 🎫 Task Breakdown

### UI Components

* **\[#001] Filter Builder Panel** (4 hrs)

  * Accordion sections for: Temporal, Content, Sentiment, Political, User, Engagement, Network
  * Chips display for active filters
  * Acceptance: Selecting controls displays chips in header
  * Dependencies: sample JSON schema for filters

* **\[#002] Query Preview & Sample Data** (3 hrs)

  * Static sample tables (posts, users, reactions)
  * Display top 5 rows matching filter state (filters tied to sample subset)
  * Acceptance: Changing filter toggles sample rows accordingly

* **\[#003] Visualization Quick‑Look** (3 hrs)

  * Mini line chart showing daily count (mock data)
  * Use Streamlit’s `st.line_chart` with sample time series
  * Acceptance: Reacts to date‑range filter

* **\[#004] Export & Templates Panel** (3 hrs)

  * Show estimated record count & data footprint (hard‑coded logic)
  * Export button triggers download of sample CSV/Parquet
  * Saved‑query dropdown loads predefined filter sets
  * Acceptance: Template selection applies filters; Export downloads file

### Static Assets & Data

* **\[#005] Prepare Sample CSV/Parquet Files** (2 hrs)

  * Create 3 sample files: `posts.csv`, `users.csv`, `engagement.csv`
  * Ensure variety of valence, toxicity, slant, timestamps
  * Acceptance: Files load into demo and power preview/export

* **\[#006] JSON Schema for Saved Queries** (1 hr)

  * Define 3 templates (e.g. toxic‑posts, cohort‑engagement, topic‑time)
  * Acceptance: Frontend can parse and apply template filters

### Integration & Polish

* **\[#007] Wire up Streamlit Layout & Navigation** (4 hrs)

  * Combine panels into cohesive layout: sidebar vs main vs bottom
  * Include app title, description, help tooltips
  * Acceptance: Navigable demo with clear labels

* **\[#008] User Onboarding & Tooltips** (2 hrs)

  * Add contextual tooltips for each filter group
  * Include a “Getting Started” collapsible help pane
  * Acceptance: Tooltips appear on hover or click

* **\[#009] Documentation & README** (2 hrs)

  * Write usage guide, sample queries, and file naming convention
  * Instructions for running `streamlit run app.py`
  * Acceptance: New user can launch demo without support

## 4. ✅ Acceptance Criteria

* **Usability:** New users can build a filter, preview samples, and simulate export within 5 min
* **Interactivity:** All UI controls respond visually and update preview or chart
* **Downloadable Demo:** Export button downloads correct sample file
* **Templates:** At least 3 saved‑query templates load correctly
* **Documentation:** README with setup and usage instructions

## 5. 🔍 Example Queries / Use Cases

| Query                                                       | Expected Demo Behavior                               |
| ----------------------------------------------------------- | ---------------------------------------------------- |
| “# toxic posts last month”                                  | Date filter + Toxicity=Yes → show sample toxic posts |
| “Political posts liked by left‑leaners”                     | Political=Yes, Cohort=Left → preview engagement rows |
| “#climatechange mentions over time”                         | Keyword=climate change + date range → chart updates  |
| “Retweet network 2 hops for #FreeSpeech”                    | Set Network=2 hops → preview mock edge list          |
| “Sentiment distribution for verified vs non‑verified users” | Cohort filter + sentiment → bar chart mock           |

## 6. 📦 Data Models & Schemas (Demo Samples)

```yaml
Post {
  id: string
  user_id: string
  text: string
  timestamp: datetime
  valence: enum(positive, neutral, negative)
  toxicity: bool
  political: bool
  slant: enum(left, center, right, unclear)
}

User {
  id: string
  handle: string
  verified: bool
  follower_count: int
  inferred_lean: enum(left, right, center, unclear)
}

Engagement {
  post_id: string
  user_id: string
  type: enum(like, retweet, comment)
  timestamp: datetime
}
```

## 7. ⏳ Timeline & Resources

* **Duration:** 2 weeks (June 7–21, 2025)
* **Owner:** 1 frontend engineer (Streamlit) + 0 backend
* **Dependencies:** Design mockups, sample data files, template definitions
* **Milestones:**

  * 2025‑06‑10: Basic filter + preview panels complete
  * 2025‑06‑14: Charts + export functionality integrated
  * 2025‑06‑18: Templates + onboarding finished
  * 2025‑06‑21: Demo polish & stakeholder review

## 8. 📦 Deliverables

* ✅ `app.py` (Streamlit demo)
* ✅ Sample data files (`*.csv`, `*.parquet`)
* ✅ README with setup & usage guide
* ✅ Mock templates JSON
* ✅ Demo video script or slides (optional)

## Example questions and desired flow

1. “What is the daily volume of toxic posts over the past year?”
    * Pain Point
      * Manually writing ETL jobs to filter by date & toxicity
      * Waiting hours for a full-year scan through TBs of JSON
    * User Flow
      * In Temporal panel: select “Jan 1 2024 – Dec 31 2024.”
      * In Sentiment & Moderation: check “Toxic.”
      * Click Preview → see sample toxic posts + estimated daily counts.
      * In Visualization Quick-Look: view auto-generated daily time series.
      * Hit Export as “ToxicPosts_2024.parquet.”
    * Value Add
      * >95% time saved vs. hand-rolling scripts
      * Instant chart validates data coverage before full export
      * Export includes schema + provenance for reproducibility

2. "How do engagement patterns differ between left-leaning vs. right-leaning users on political posts?"
    * Pain Point
      * Needing to join user-profile metadata (slant) with engagement logs
      * Writing two separate queries and merging results offline
    * User Flow
      * Content: toggle "Political = Yes."
      * Political Attributes: check "Left" and "Right."
      * Engagement → "Liked by cohort" → select "Left-leaning," then add another filter for "Right-leaning."
      * Preview shows side-by-side counts.
      * Export two slices or a combined table with a cohort column.
    * Value Add
      * No SQL/joins: one pipeline handles both cohorts
      * Parallel preview lets you eyeball differences before export
      * Data arrive analysis-ready with a built-in cohort flag

3. "What is the average sentiment trajectory of users before and after a major political event?"
    * Pain Point
      * Complex window-function calculations over each user's timeline
      * Pulling multi-day snapshots and computing moving averages offline
    * User Flow
      * Content: filter any posts/comments
      * Temporal: choose date windows (e.g. 7 days before/after event)
      * Sentiment: include Positive/Neutral/Negative
      * User Cohort: leave blank (all users)
      * In Visualization, select "Avg valence over time per user" preview
      * Export aggregated CSV with columns [user, date, avg_valence]
    * Value Add
      * Built-in aggregation removes need for external pandas code
      * Preview chart confirms correct window alignment
      * Exported CSV plugs straight into statistical software (R, Stata)
4. "How many unique users discuss 'climate change' each month?"
    * Pain Point
      * Keyword vs. semantic topic matching scripts are brittle
      * Counting unique user_ids across many partitions manually
    * User Flow
      * Content: type keyword "climate change" (supports fuzzy matching)
      * Topic taxonomy: confirm "Climate" tag
      * Temporal: monthly binning
      * Preview shows a table of [month, unique_users]
      * Export as Parquet for longitudinal analysis
    * Value Add
      * Hybrid keyword+topic matching ensures recall & precision
      * Unique-user aggregation built into export
      * Instant QC via table preview avoids wasted compute

5. "What proportion of political posts are also classified as toxic?"
    * Pain Point
      * Joining political flags, and toxicity labels
      * Scripting multi-level aggregations over millions of records
    * User Flow
      * Political = Yes; Toxicity: both Toxic & Not Toxic
      * Select "Pivot export" → get [political_count, toxic_count, pct_toxic]
    * Value Add
      * Multi-dimensional pivot right in UI
      * Export yields ready-to-plot summary table

6. "How does a hashtag (e.g., #FreeSpeech) diffuse through the retweet network?"
    * Pain Point
      * Running graph-traversals on massive edge lists
      * Installing/configuring Neo4j or custom Spark jobs
    * User Flow
      * Content: filter posts containing #FreeSpeech
      * Network: set "Retweet graph distance = 3 hops"
      * Preview: small subgraph JSON + node counts
      * Export graph in edge-list format for Gephi or NetworkX
    * Value Add
      * No graph-DB required: network depth baked into export
      * Sampling preview ensures scale is manageable
      * Rapid iteration on hop-limits in a few clicks

7. "Which user cohorts most frequently retweet misinformation-labeled content?"
    * Pain Point
      * Aligning external misinformation labels with internal post IDs
      * Segmenting users by follower-count, verification, or lean
    * User Flow
      * Content: upload or select "Misinformation = Yes"
      * Engagement: "Retweeted by cohort" → choose follower-count buckets or verification status
      * Preview table: [cohort, retweet_count]
      * Export normalized rates per 1,000 users
    * Value Add
      * One UI merges external label sets and cohort filters
      * Rates computed on the fly—no manual denominators
      * Export directly imports to visualization notebooks

8. "How do reply-network structures differ for toxic vs. non-toxic posts?"
    * Pain Point
      * Building two separate network extracts and comparing metrics (e.g., degree distributions) offline
    * User Flow
      * Sentiment: select "Toxic" + "Not Toxic" (under separate saved-query templates)
      * Engagement: "Commented by" → select "Replies"
      * Network: set distance = 1 hop to get direct reply edges
      * Preview summary stats (avg degree, cluster count)
      * Export two graph datasets for side-by-side analysis
    * Value Add
      * Side-by-side template execution vs. manual reconfiguration
      * Inline network stats for sanity checking
      * Export ready for network-analysis libraries

9. "What is the correlation between toxicity and virality (likes/retweets)?"
    * Pain Point
      * Merging toxicity scores with engagement tallies
      * Computing correlation coefficients in Python or R
    * User Flow
      * Sentiment & Moderation: include toxicity labels
      * Engagement: add "Likes," "Retweets" columns
      * Preview small sample of [post_id, toxic_flag, like_count, retweet_count]
      * Export as CSV with those four fields
    * Value Add
      * Single-step join of content + engagement metrics
      * Immediate sample sanity check avoids downstream errors
      * Export plugs into your statistical analysis script

10. "How does sentiment differ between verified vs. non-verified users?"
    * Pain Point
      * Tagging verification status onto every post record
      * Filtering and aggregating sentiment by that tag
    * User Flow
      * User Cohort: dropdown "Verified" or "Non-verified"
      * Sentiment: choose Positive/Neutral/Negative
      * Preview bar chart of sentiment distribution per cohort
      * Export wide-format table [cohort, pos_pct, neu_pct, neg_pct]
    * Value Add
      * No external profile-join necessary: cohorts are pre-hydrated
      * Inline visualization confirms cohort differences
      * Exported summary can be cited directly in papers


## UI concept sketch

1. Left-Panel “Filter Builder”
    * Accordion sections for Temporal, Content, Sentiment, Political, User, Engagement, Network.
    * Displays active filters as removable “chips” at top.

2. Central “Query Preview”
    * Shows a small random sample (e.g. 5 rows) of filtered records with key columns.
    * Enables instant sanity check: “see 5 example posts matching your criteria.”

3. Right-Panel “Export & Metrics”
    * Estimated result size (count of posts, users, reactions).
    * Data footprint estimate (MB/GB) based on average record size.
    * Export format selector (CSV, Parquet, R-dataframe).
    * One-click Export button, plus “Save query” for reproducibility.

4. Top Bar “Saved Queries & Templates”
    * Dropdown of common research templates (e.g. “Toxic political posts by cohort”).
    * Ability to share saved queries with collaborators.

5. Bottom “Visualization Quick-Look”
    * Mini line chart showing, e.g., daily count over the selected date range.
    * Helps validate “X over time” questions before export.

## Use-case prioritization

| Priority | Use Case                                                            | Why First?                                               |
| -------- | ------------------------------------------------------------------- | -------------------------------------------------------- |
| **1**    | **Basic counts & trends** (e.g. “# toxic posts,” “topic over time”) | Most researchers start by exploratory counts & plots.    |
| **2**    | **Cohort engagement** (“political posts liked by left-leaners”)     | Widely used in polarization and consumption studies.     |
| **3**    | **Valence trajectories** (avg sentiment per user/day)               | Captures temporal dynamics of affect—key in many papers. |
| **4**    | **Network diffusion** (1–n hop spread of content)                   | More advanced but high-impact for echo-chamber research. |
| **5**    | **Complex join exports** (posts + reactions + user-profiles)        | Enable multivariate modeling pipelines.                  |

## Core filter fields

| Category                   | Filter                                            | Control Type               | Notes                                                                        |
| -------------------------- | ------------------------------------------------- | -------------------------- | ---------------------------------------------------------------------------- |
| **Temporal**               | Date range (post timestamp)                       | Dual-slider calendar       | Allow “from … to” selection down to the minute.                              |
|                            | Hour‐of‐day / day‐of‐week                         | Multi-select dropdown      | For studying diurnal or weekly patterns.                                     |
| **Content**                | Full-text keyword / regex                         | Text input + “Add tag”     | Tokenize multi-term, support quoted phrases.                                 |
|                            | Topic taxonomy (e.g. COVID, elections, climate)   | Tag-picker                 | Predefined list with “+ Add custom topic.”                                   |
| **Sentiment & Moderation** | Valence (Positive / Neutral / Negative)           | Checkbox group             | Allow multi-select.                                                          |
|                            | Toxicity (Toxic / Not Toxic / Uncertain)          | Toggle / tri-state button  |                                                                              |
| **Political Attributes**   | Political content (Yes / No)                      | Toggle                     |                                                                              |
|                            | Political slant (Left / Center / Right / Unclear) | Checkbox group             |                                                                              |
| **User Cohort**            | Specific handles (exact match)                    | Multi-autocomplete input   |                                                                              |
|                            | Demographic or inferred attributes                | Dropdown (e.g. lean-left)  | Support segments like “left-leaning”, “verified accounts,” “>10k followers.” |
| **Engagement**             | Liked by (handles or cohort)                      | Multi-autocomplete input   |                                                                              |
|                            | Retweeted by / commented by                       | Same as above              |                                                                              |
| **Network**                | Graph distance from seed users                    | Numeric stepper (1–5 hops) |                                                                              |
|                            | Community detection (cluster ID)                  | Dropdown                   | If you precompute Louvain clusters or similar.                               |

## Problem statement

1. Data Volume & Complexity
    * Our Bluesky data lake spans multiple terabytes of raw JSON, covering posts, profiles, reactions, and network graphs.
    * Researchers must spend days or weeks just to subset, clean, and reshape it for a single study.
2. Opportunity Cost
    * Time spent on ETL detracts from hypothesis generation, statistical modeling, and writing.
    * Delays in data prep slow publication cycles and grant deliverables.

## Solution Overview

A lightweight web application that lets researchers:

1. Visually define subsets (e.g., date ranges, user cohorts, hashtag filters, network-depth).
2. Instantly preview sample records to validate their criteria.
3. One-click export of cleaned, analysis-ready CSV or Parquet slices—fully documented with schema and provenance.

## Quantified benefits and ROI

| Metric                             | Typical Manual Workflow  | With Curated Data App              | Savings / ROI                              |
| ---------------------------------- | ------------------------ | ---------------------------------- | ------------------------------------------ |
| **Time to first dataset**          | 2–5 days                 | < 10 minutes                       | **>95% reduction** in data-prep time       |
| **Average researcher hourly rate** | \$50–\$100/hr            | (Tool cost amortized across users) | Net saving of \$200–\$400 per dataset prep |
| **Per-project prep cost**          | \$400 (4 hrs @ \$100/hr) | \$20 (app hosting & maintenance)   | **95% cost reduction**                     |
| **Turnaround for new hypotheses**  | 1–2 weeks                | 1–2 hours                          | Accelerates publication & grant milestones |
