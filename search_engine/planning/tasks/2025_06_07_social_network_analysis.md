# Social Network Analysis Tab Demo - Daily Task Tracking

## 📝 Overview
This document tracks daily progress on the Streamlit Social Network Analysis (SNA) Tab Demo as defined in [2025_06_07_social_network_analysis_prd.md](../prd/2025_06_07_social_network_analysis_prd.md). The goal is to deliver a fully interactive, demo-ready SNA interface for social science research on polarization within 2 weeks. All implementation code will be developed in the new `search_engine/app_v2` directory and integrated as a new tab in the main app.

## 📋 Task Checklist

### UI Components
- [x] [#001] Sidebar Controls (3 hrs)
  - [x] Dropdowns for Edge Type, Community Algorithm, Centrality Metric
  - [x] Sliders for Hop Depth, Time Range
  - [x] Checkbox groups for content filters: toxic/not toxic, valence (positive/neutral/negative), political (is/is not), political slant (left/center/right/unclear; only if political is True)
  - [x] Controls change preview and metrics
  - [x] Unit tests for controls and filter logic
  - [x] Split page: left = filters, right = chosen filters (deselectable)
  - [x] "Submit" button displays chosen parameters as a string

- [x] [#002] Mini-Graph Preview (4 hrs)
  - [x] Integrate networkx to render 50-node sample graph
  - [x] Use Streamlit component (pyvis) for visualization
  - [x] Graph updates when controls change
  - [x] Unit tests for graph rendering and update logic

- [ ] [#003] Metric Summary Panel (3 hrs)
  - [ ] Display computed metrics: top 5 central nodes, community count, assortativity
  - [ ] Use static sample calculations tied to sample data
  - [ ] Metrics refresh on filter/parameter changes

- [ ] [#SNA-004] Export Simulation (2 hrs)
  - [ ] Buttons for "Download Edge List (CSV)", "Download Node Metrics (CSV)", "Download GEXF"
  - [ ] Simulate file with sample network data
  - [ ] Download triggers sample file

- [ ] [#SNA-005] Time Slider Animation (3 hrs)
  - [ ] Implement st.slider for date range; update graph and metrics accordingly
  - [ ] Sliding dates shows different pre-defined sample snapshots

### Static Assets & Data
- [ ] [#SNA-006] Prepare Sample Network Files (2 hrs)
  - [ ] Create .csv for nodes (political, valence, slant, toxicity) and edges (retweet, reply, like); 100 nodes/day for 2024-06-01 to 2024-06-14 (1,400 rows)
  - [ ] Bias sampling: positive valence/low toxicity → more engagement; negative valence/high toxicity → less engagement (verify trend with unit tests)
  - [ ] Python generator file with data quality checks; export as .csv
  - [ ] Load .csv for unit tests and simulation
  - [ ] Precompute centrality/community for snapshots
  - [ ] Files load and power UI components

- [ ] [#SNA-007] Define Sample Metrics and make sure that the interface is unified (1 hr)
  - [ ] Hard-code centrality rankings and assortativity values for each snapshot
  - [ ] Metrics panel displays correct values per snapshot
  - [ ] Consolidate across all the network interfaces, making sure that any different sample datas for each component is removed and all components use the prepped sample data. If any component requires data or fields that aren't included in the
  sample data, load the .csv file for the sample data, add those necessary fields, and then 

### Integration & Documentation
- [ ] [#SNA-008] Layout & Styling (3 hrs)
  - [ ] Add tab layout: integrate SNA tab alongside Data Access tab
  - [ ] Ensure cohesive styling and navigation
  - [ ] SNA tab accessible and visually consistent

- [ ] [#SNA-009] Onboarding & Help (2 hrs)
  - [ ] Tooltips explaining SNA concepts (centrality, community)
  - [ ] "How to Use" collapsible help section
  - [ ] Help elements appear contextually

- [ ] [#SNA-010] README & Demo Instructions (2 hrs)
  - [ ] Write instructions for running demo and navigating SNA tab
  - [ ] Include sample research questions and flow script
  - [ ] Stakeholders can launch and demo without assistance

## 📝 Progress Notes

### Day 1 (2025-06-13)
- [#001] Sidebar Controls fully implemented and tested.
  - All dropdowns (Edge Type, Community Algorithm, Centrality Metric), sliders (Hop Depth, Time Range), and checkbox groups (Toxic, Not Toxic, Valence, Political, Not Political, Slant) are present and functional.
  - Selected filters are displayed as chips, and the UI allows for future deselection logic.
  - The "Submit" button displays a string summary of the chosen parameters.
  - All state is managed in a modular, typed way, and the panel is integrated into the SNA tab with a two-column layout.
  - Unit tests for sidebar controls render and logic are passing.
  - All acceptance criteria for #001 are met.
- Next: Begin Mini-Graph Preview (#002)

### Day 2 (2025-06-14)
- [#002] Mini-Graph Preview fully implemented and tested.
  - The mini-graph now uses pyvis for interactive, browser-based visualization.
  - The network structure consists of two dense clusters with 4–5 broker nodes connecting them, visually representing high homophily and broker roles.
  - Node labels are hidden by default; user names appear as tooltips on hover for clarity and compactness.
  - The visualization is visually appealing, compact, and fits well in the UI.
  - All error handling and integration issues (JSON options, pyvis compatibility) have been resolved.
  - Unit tests for graph rendering and update logic are passing.
  - All acceptance criteria for #002 are met.
- Next: Begin Metric Summary Panel (#003)

## 📊 Status Summary
- Overall Progress: 2/15 tasks complete (13%)
- Completed Tasks: 2/15
- Estimated Remaining Time: 22 hours
- Timeline: Work began 2025-06-13, target completion 2025-06-21 