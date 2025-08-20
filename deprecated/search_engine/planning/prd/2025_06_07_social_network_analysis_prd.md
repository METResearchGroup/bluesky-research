# PRD: Social Network Analysis Tab Demo (Streamlit)

## 1. 🧩 Objective

* **Goal:** Build a Streamlit demo showcasing a Social Network Analysis (SNA) interface for social science researchers studying polarization.
* **Success Metrics:** Within 5 minutes, users can configure graph parameters, view a mini-network visualization, inspect key SNA metrics, and simulate data export.
* **Timeline:** Prototype complete by **2025-06-21**.

## 2. 📌 Scope

### ✅ In-Scope

* Streamlit front-end UI for SNA tab
* Controls for edge type, filters, graph parameters, and time slider
* Static sample datasets for nodes and edges (50–100 nodes, sample attributes)
* Mini-graph preview (force-directed layout using networkx + pyvis or Streamlit network component)
* Metric summary panel (centrality, community counts, assortativity)
* Simulated export buttons (CSV edge list, node metrics, GEXF)

### ❌ Out-of-Scope

* Real-time computation on live data
* Backend graph databases or large-scale network processing
* Dynamic LLM or R integration for network insights
* Authentication or multi-user collaboration

## 3. 🎫 Task Breakdown

### UI Components

* **\[#001] Sidebar Controls** (3 hrs)

  * Dropdowns for Edge Type, Community Algorithm, Centrality Metric
  * Sliders for Hop Depth, Time Range
  * Checkbox groups for content filters:
    * toxic/not toxic
    * valence: positive, neutral, negative
    * political: is/is not political
    * political slant: left/center/right/unclear (only applicable to cases where political is True).
  * Acceptance: Controls change preview and metrics
  * Create unit tests where appropriate, in order to verify functionality.
  * Split the page into two halves, the left being the filters and the right
  displaying the list of filters that have been chosen. Allow the users to also
  deselect the filters that have been chosen, by clicking on the list of filters
  that have been chosen.
  * Include a "Submit" button that displays a string message telling the user what parameters they've chosen.

* **\[#002] Mini-Graph Preview** (4 hrs)

  * Integrate networkx to render 50-node sample graph
  * Use Streamlit component (e.g., `st.graphviz_chart` or `pyvis`) for visualization
  * Acceptance: Graph updates when controls change
  * Create unit tests where appropriate, in order to verify functionality.

* **\[#003] Metric Summary Panel** (3 hrs)

  * Display computed metrics: top 5 central nodes, community count, assortativity
  * Use static sample calculations tied to sample data
  * Acceptance: Metrics refresh on filter/parameter changes

* **\[#SNA-004] Export Simulation** (2 hrs)

  * Buttons for “Download Edge List (CSV)”, “Download Node Metrics (CSV)”, “Download GEXF”
  * Simulate file with sample network data
  * Acceptance: Download triggers sample file

* **\[#SNA-005] Time Slider Animation** (3 hrs)

  * Implement `st.slider` for date range; update graph and metrics accordingly
  * Acceptance: Sliding dates shows different pre-defined sample snapshots

### Static Assets & Data

* **\[#SNA-006] Prepare Sample Network Files** (2 hrs)

  * Create .csv for nodes with attributes (political, valence, slant, toxicity) and edges (retweet, reply, like). Make sure that there are at least 100 nodes per day for the date range 2024-06-01 to 2024-06-14 (so, total of 1,400 rows). Bias the sampling so that across each day, the probability of a node engaging with (retweet, reply, like) another node with positive valence and low toxicity goes up (irrespective of political or slant), whereas the probability of a node engaging with another node goes down with the presence of negative valence and higher toxicity. Verify with unit tests that this trend is True when comparing day N to day N + 1, for all N days. Use a Python file to generate this data, make sure that the Python generator file includes the requisite data quality checks, export it as a .csv file, and then load in that .csv file for unit tests and then for the simulation itself.
  * Precompute centrality/community for snapshots
  * Acceptance: Files load and power UI components

* **\[#SNA-007] Define Sample Metrics** (1 hr)

  * Hard-code centrality rankings and assortativity values for each snapshot
  * Acceptance: Metrics panel displays correct values per snapshot

### Integration & Documentation

* **\[#SNA-008] Layout & Styling** (3 hrs)

  * Add tab layout: integrate SNA tab alongside Data Access tab
  * Ensure cohesive styling and navigation
  * Acceptance: SNA tab accessible and visually consistent

* **\[#SNA-009] Onboarding & Help** (2 hrs)

  * Tooltips explaining SNA concepts (centrality, community)
  * “How to Use” collapsible help section
  * Acceptance: Help elements appear contextually

* **\[#SNA-010] README & Demo Instructions** (2 hrs)

  * Write instructions for running demo and navigating SNA tab
  * Include sample research questions and flow script
  * Acceptance: Stakeholders can launch and demo without assistance

## 4. ✅ Acceptance Criteria

* Users can select edge type, filters, and graph parameters and see updates
* Mini-graph preview renders correctly for sample data
* Metric summary shows centrality, community count, and assortativity
* Time slider changes graph and metrics according to snapshots
* Export buttons download correct sample files
* Help tooltips clarify SNA concepts

## 5. 🔍 Example SNA Research Questions (Demo Scripts)

1. **Brokers Between Partisan Communities**: identify top betweenness nodes between left/right clusters
2. **Diffusion Speed of Toxic vs. Neutral Content**: compare average path lengths of toxic vs. neutral cascades
3. **Bipartisan Reply Ties Around Events**: animate reply-network before & after debate
4. **Hashtag Cascade Reach**: trace #Hashtag spread over 3 hops and count distinct communities
5. **Homophily Metric**: display network assortativity by political slant
6. **Degree Centrality Spike**: show top degree change on a key date
7. **Structural Holes**: list nodes with lowest constraint values
8. **Community Shift Pre/Post Event**: compare number and size of clusters
9. **Misinformation Super-spreaders**: rank nodes by betweenness on misinformation retweet network
10. **Network Density Over Time**: line chart of density metric across date range

## 6. 📦 Data Models & Schemas (Samples)

```yaml
Node {
  id: str
  political_content_rate: float
  slant: enum(left, center, right, unclear)
  toxicity_rate: float
  average_valence: float # if float > 0.2, then it is a positive node, else -0.2 < float <= 0.2 then neutral, else negative
}
Edge {
  source: str
  target: str
  type: enum(retweet, reply, mention, follow)
  timestamp: datetime
}
```

## 7. ⏳ Timeline & Resources

* **Duration:** 2 weeks (Jun 7–21, 2025)
* **Owner:** 1 frontend engineer
* **Dependencies:** Sample network data, SNA metric definitions
* **Milestones:**

  * Jun 10: Controls & preview
  * Jun 14: Metrics & export
  * Jun 18: Time slider & help
  * Jun 21: Demo review

## 8. 📦 Deliverables

* ✅ `sna_app.py` with SNA tab demo
* ✅ Sample network CSV/JSON and snapshot metrics
* ✅ README with SNA usage guide and scripts
* ✅ Demo slide deck or video script
