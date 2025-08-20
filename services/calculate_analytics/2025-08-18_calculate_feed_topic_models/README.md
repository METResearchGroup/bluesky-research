# Analysis, 2025-08-18

Goal: Discover “what people were shown” thematically, and whether those themes differ by experimental condition (your 3 conditions) and, optionally, over time (Sept–Dec).

Tooling idea: Use BERTopic (a clustering + embedding topic model). Its “dynamic” mode usually tracks topics over time; your colleague suggests reusing the “timepoint” slot to mean “group/condition” so you can compare topic prevalence across conditions.

Output you want:

A set of human-readable topics (labels + exemplar posts), and

For each topic, its relative prevalence per condition (and optionally per month), with visualizations and simple stats showing which topics are enriched/depleted in each condition.
