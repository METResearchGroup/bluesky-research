"""Experimentation file to help understand Named Entity Recognition (NER) under the hood.

This file demonstrates NER concepts for MET-52: Implement Named Entity Recognition (NER) analysis.
It shows entity extraction, normalization, surface form tracking, and frequency analysis.
"""

import spacy
import pandas as pd
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Set
import re
import random
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json
import os
import csv
from lib.constants import timestamp_format, current_datetime_str

# Sample political/sociopolitical posts for experimentation (50 posts)
posts = [
    {"uri": "at://did:plc:abc123/post1", "text": "Election officials report high turnout in Arizona and Georgia counties."},
    {"uri": "at://did:plc:abc123/post2", "text": "President Biden and Vice President Harris discuss infrastructure policy."},
    {"uri": "at://did:plc:abc123/post3", "text": "The Supreme Court ruled on abortion rights in Mississippi yesterday."},
    {"uri": "at://did:plc:abc123/post4", "text": "Senator Warren from Massachusetts criticized corporate tax loopholes."},
    {"uri": "at://did:plc:abc123/post5", "text": "Governor DeSantis of Florida signed new education legislation today."},
    {"uri": "at://did:plc:abc123/post6", "text": "The Federal Reserve announced interest rate changes affecting markets."},
    {"uri": "at://did:plc:abc123/post7", "text": "Congressional Democrats and Republicans debated healthcare reform."},
    {"uri": "at://did:plc:abc123/post8", "text": "Mayor Adams of New York City addressed crime statistics publicly."},
    {"uri": "at://did:plc:abc123/post9", "text": "The Pentagon confirmed military operations in Afghanistan continue."},
    {"uri": "at://did:plc:abc123/post10", "text": "Secretary Blinken met with European Union leaders in Brussels."},
    {"uri": "at://did:plc:abc123/post11", "text": "Senator Cruz from Texas opposed the climate change bill."},
    {"uri": "at://did:plc:abc123/post12", "text": "The White House announced new immigration policies this week."},
    {"uri": "at://did:plc:abc123/post13", "text": "Governor Newsom of California signed environmental protection laws."},
    {"uri": "at://did:plc:abc123/post14", "text": "The Department of Justice investigated antitrust violations."},
    # {"uri": "at://did:plc:abc123/post15", "text": "Mayor Lightfoot of Chicago discussed urban development plans."},
    # {"uri": "at://did:plc:abc123/post16", "text": "Senator Sanders from Vermont proposed Medicare for All legislation."},
    # {"uri": "at://did:plc:abc123/post17", "text": "The Department of Education announced new student loan forgiveness programs."},
    # {"uri": "at://did:plc:abc123/post18", "text": "Governor Abbott of Texas signed controversial voting restrictions into law."},
    # {"uri": "at://did:plc:abc123/post19", "text": "The Environmental Protection Agency released new climate regulations."},
    # {"uri": "at://did:plc:abc123/post20", "text": "Senator McConnell from Kentucky blocked infrastructure bill negotiations."},
    # {"uri": "at://did:plc:abc123/post21", "text": "The Department of Homeland Security addressed border security concerns."},
    # {"uri": "at://did:plc:abc123/post22", "text": "Mayor Garcetti of Los Angeles announced new housing initiatives."},
    # {"uri": "at://did:plc:abc123/post23", "text": "The Federal Bureau of Investigation investigated election interference claims."},
    # {"uri": "at://did:plc:abc123/post24", "text": "Senator Klobuchar from Minnesota supported renewable energy investments."},
    # {"uri": "at://did:plc:abc123/post25", "text": "The Department of Transportation announced infrastructure funding allocations."},
    # {"uri": "at://did:plc:abc123/post26", "text": "Governor Whitmer of Michigan signed clean energy legislation."},
    # {"uri": "at://did:plc:abc123/post27", "text": "The Department of Health and Human Services addressed healthcare access issues."},
    # {"uri": "at://did:plc:abc123/post28", "text": "Senator Booker from New Jersey proposed criminal justice reform bills."},
    # {"uri": "at://did:plc:abc123/post29", "text": "The Department of Labor announced new worker protection regulations."},
    # {"uri": "at://did:plc:abc123/post30", "text": "Mayor Wheeler of Portland addressed homelessness and public safety."},
    # {"uri": "at://did:plc:abc123/post31", "text": "The Department of Agriculture released new farm subsidy programs."},
    # {"uri": "at://did:plc:abc123/post32", "text": "Senator Gillibrand from New York supported paid family leave legislation."},
    # {"uri": "at://did:plc:abc123/post33", "text": "The Department of Commerce announced trade policy updates with China."},
    # {"uri": "at://did:plc:abc123/post34", "text": "Governor Polis of Colorado signed marijuana legalization expansion."},
    # {"uri": "at://did:plc:abc123/post35", "text": "The Department of Energy invested in clean technology research."},
    # {"uri": "at://did:plc:abc123/post36", "text": "Senator Duckworth from Illinois supported veteran healthcare improvements."},
    # {"uri": "at://did:plc:abc123/post37", "text": "The Department of Veterans Affairs announced new mental health services."},
    # {"uri": "at://did:plc:abc123/post38", "text": "Mayor Breed of San Francisco addressed tech industry regulation."},
    # {"uri": "at://did:plc:abc123/post39", "text": "The Department of Housing and Urban Development released affordable housing grants."},
    # {"uri": "at://did:plc:abc123/post40", "text": "Senator Cortez Masto from Nevada supported immigration reform efforts."},
    # {"uri": "at://did:plc:abc123/post41", "text": "The Department of Interior announced national park conservation initiatives."},
    # {"uri": "at://did:plc:abc123/post42", "text": "Governor Inslee of Washington signed climate action legislation."},
    # {"uri": "at://did:plc:abc123/post43", "text": "The Department of State addressed international diplomatic relations."},
    # {"uri": "at://did:plc:abc123/post44", "text": "Senator Baldwin from Wisconsin supported manufacturing job creation."},
    # {"uri": "at://did:plc:abc123/post45", "text": "The Department of Treasury announced economic stimulus package details."},
    # {"uri": "at://did:plc:abc123/post46", "text": "Mayor Johnson of Dallas addressed police reform and community relations."},
    # {"uri": "at://did:plc:abc123/post47", "text": "The Department of Defense confirmed military budget allocations."},
    # {"uri": "at://did:plc:abc123/post48", "text": "Senator Rosen from Nevada supported renewable energy tax credits."},
    # {"uri": "at://did:plc:abc123/post49", "text": "The Department of Justice announced civil rights enforcement priorities."},
    # {"uri": "at://did:plc:abc123/post50", "text": "Governor Evers of Wisconsin signed education funding increases."}
]

# Entity normalization mappings (common aliases)
ENTITY_ALIASES = {
    # People
    "joseph biden": "joe biden",
    "kamala harris": "kamala harris",
    "elizabeth warren": "elizabeth warren",
    "ron desantis": "ron desantis",
    "eric adams": "eric adams",
    "anthony blinken": "antony blinken",
    "ted cruz": "ted cruz",
    "gavin newsom": "gavin newsom",
    "lori lightfoot": "lori lightfoot",
    
    # Organizations
    "supreme court": "supreme court",
    "federal reserve": "federal reserve",
    "pentagon": "pentagon",
    "white house": "white house",
    "department of justice": "department of justice",
    "doj": "department of justice",
    
    # Locations
    "united states": "usa",
    "usa": "usa",
    "us": "usa",
    "new york city": "new york",
    "nyc": "new york",
    "california": "california",
    "ca": "california",
    "texas": "texas",
    "tx": "texas",
    "florida": "florida",
    "fl": "florida",
    "massachusetts": "massachusetts",
    "ma": "massachusetts",
    "mississippi": "mississippi",
    "ms": "mississippi",
    "arizona": "arizona",
    "az": "arizona",
    "georgia": "georgia",
    "ga": "georgia",
    "chicago": "chicago",
    "brussels": "brussels",
    "afghanistan": "afghanistan",
    "european union": "european union",
    "eu": "european union"
}

def normalize_entity(entity_text: str) -> str:
    """
    Normalize entity text using case-folding, punctuation stripping, and alias mapping.
    
    Args:
        entity_text: Raw entity text from spaCy
        
    Returns:
        Normalized entity text
    """
    # Case-folding
    normalized = entity_text.lower()
    
    # Strip surrounding punctuation
    normalized = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', normalized)
    
    # Map common aliases
    normalized = ENTITY_ALIASES.get(normalized, normalized)
    
    return normalized.strip()

def extract_entities_with_spacy(posts: List[Dict], model_name: str = "en_core_web_sm") -> Dict[str, List[Dict]]:
    """
    Extract entities from posts using spaCy NER pipeline.
    
    Args:
        posts: List of dictionaries with "uri" and "text" keys
        model_name: spaCy model name
        
    Returns:
        Dictionary mapping URI to list of entity dictionaries with entity_type and entity_normalized
    """
    try:
        nlp = spacy.load(model_name)
    except OSError:
        print(f"Model {model_name} not found. Please install with: python -m spacy download {model_name}")
        return {}
    
    # Focus on political/sociopolitical entity types
    target_types = {"PERSON", "ORG", "GPE", "DATE"}
    
    uri_to_entities = {}
    
    for post in posts:
        uri = post["uri"]
        text = post["text"]
        
        doc = nlp(text)
        post_entities = []
        
        for ent in doc.ents:
            if ent.label_ in target_types:
                entity_info = {
                    "entity_type": ent.label_,
                    "entity_normalized": normalize_entity(ent.text)
                }
                post_entities.append(entity_info)
        
        uri_to_entities[uri] = post_entities
    
    return uri_to_entities

def analyze_entity_frequencies(uri_to_entities: Dict[str, List[Dict]], frequency_threshold: int = 1) -> Dict:
    """
    Analyze entity frequencies and create stratified analysis.
    
    Args:
        uri_to_entities: Dictionary mapping URI to list of entity dictionaries
        frequency_threshold: Minimum frequency to include entity
        
    Returns:
        Dictionary with frequency analysis results
    """
    # Count normalized entities across all posts
    entity_counts = Counter()
    entity_types = defaultdict(set)  # normalized -> set of entity types
    
    for uri, entities in uri_to_entities.items():
        for entity in entities:
            normalized = entity["entity_normalized"]
            entity_type = entity["entity_type"]
            
            entity_counts[normalized] += 1
            entity_types[normalized].add(entity_type)
    
    # Filter by frequency threshold
    filtered_entities = {
        entity: count for entity, count in entity_counts.items() 
        if count >= frequency_threshold
    }
    
    # Create stratified analysis
    analysis = {
        "overall": {
            "total_entities": sum(entity_counts.values()),
            "unique_normalized": len(filtered_entities),
            "top_entities": dict(Counter(filtered_entities).most_common(10))
        },
        "by_type": defaultdict(lambda: {"count": 0, "entities": []}),
        "entity_mapping": {
            entity: {
                "count": count,
                "entity_types": list(entity_types[entity])
            }
            for entity, count in filtered_entities.items()
        }
    }
    
    # Analyze by entity type
    for uri, entities in uri_to_entities.items():
        for entity in entities:
            entity_type = entity["entity_type"]
            normalized = entity["entity_normalized"]
            
            if normalized in filtered_entities:
                analysis["by_type"][entity_type]["count"] += 1
                analysis["by_type"][entity_type]["entities"].append(normalized)
    
    return analysis

def create_extended_hash_map(uri_to_entities: Dict[str, List[Dict]], posts: List[Dict]) -> Dict:
    """
    Create extended hash map format as specified in ticket requirements.
    
    Format: {"<date>": {"<condition>": [{"entity_normalized":"<keyword>","entity_raws":["..."],"count":<count>}]}}
    
    Args:
        uri_to_entities: Dictionary mapping URI to list of entity dictionaries
        posts: List of post dictionaries with "uri" and "text" keys
        
    Returns:
        Extended hash map structure
    """
    # Simulate dates and conditions for demonstration
    dates = ["2024-01-15", "2024-01-16", "2024-01-17"]
    conditions = ["control", "treatment_a", "treatment_b"]
    
    hash_map = {}
    
    # Group entities by simulated date and condition
    for i, post in enumerate(posts):
        uri = post["uri"]
        entities = uri_to_entities.get(uri, [])
        
        # Simulate date assignment
        date = dates[i % len(dates)]
        condition = conditions[i % len(conditions)]
        
        if date not in hash_map:
            hash_map[date] = {}
        
        if condition not in hash_map[date]:
            hash_map[date][condition] = []
        
        # Process entities for this post
        for entity in entities:
            # Check if entity already exists in this date/condition
            existing_entity = None
            for existing in hash_map[date][condition]:
                if existing["entity_normalized"] == entity["entity_normalized"]:
                    existing_entity = existing
                    break
            
            if existing_entity:
                # Update existing entity
                existing_entity["count"] += 1
            else:
                # Add new entity
                hash_map[date][condition].append({
                    "entity_normalized": entity["entity_normalized"],
                    "entity_raws": [entity["entity_normalized"]],  # Using normalized as raw for simplicity
                    "count": 1
                })
    
    return hash_map

def export_to_csv_format(uri_to_entities: Dict[str, List[Dict]], posts: List[Dict], filename_prefix: str = "ner_analysis") -> str:
    """
    Export entities to standardized CSV format.
    
    Format: date, condition, entity_normalized, entity_raws (comma-separated), count, pre_post_flag
    
    Args:
        uri_to_entities: Dictionary mapping URI to list of entity dictionaries
        posts: List of post dictionaries with "uri" and "text" keys
        filename_prefix: Prefix for filename
        
    Returns:
        CSV content as string
    """
    # Simulate dates, conditions, and pre/post flags
    dates = ["2024-01-15", "2024-01-16", "2024-01-17"]
    conditions = ["control", "treatment_a", "treatment_b"]
    pre_post_flags = ["pre", "post"]
    
    csv_data = []
    
    for i, post in enumerate(posts):
        uri = post["uri"]
        entities = uri_to_entities.get(uri, [])
        
        date = dates[i % len(dates)]
        condition = conditions[i % len(conditions)]
        pre_post = pre_post_flags[i % len(pre_post_flags)]
        
        for entity in entities:
            csv_data.append({
                "date": date,
                "condition": condition,
                "entity_normalized": entity["entity_normalized"],
                "entity_raws": entity["entity_normalized"],  # Using normalized as raw for simplicity
                "count": 1,
                "pre_post_flag": pre_post
            })
    
    # Convert to DataFrame and then to CSV string
    df = pd.DataFrame(csv_data)
    csv_content = df.to_csv(index=False)
    
    return csv_content

def create_user_feeds():
    """
    Create 6 users across 3 conditions with 2 feeds each (pre/post-election).
    Each feed contains 5 randomly sampled posts.
    
    Returns:
        user_feeds: {"<user_id>": {"<date>": [<list of post URIs>]}}
        user_conditions: {"<user_id>": "<condition>"}
    """
    conditions = ["reverse_chronological", "engagement", "representative_diversification"]
    feed_dates = ["2024-10-01", "2024-11-25"]  # pre-election, post-election
    
    user_feeds = {}
    user_conditions = {}
    
    random.seed(42)  # For reproducible results
    
    for i in range(6):
        user_id = f"user_{i+1}"
        condition = conditions[i % 3]
        
        user_conditions[user_id] = condition
        
        user_feeds[user_id] = {}
        for feed_date in feed_dates:
            # Randomly sample 5 posts for this feed (from the 14 active posts)
            sampled_posts = random.sample(posts, min(5, len(posts)))
            # Extract just the URIs
            post_uris = [post["uri"] for post in sampled_posts]
            user_feeds[user_id][feed_date] = post_uris
    
    return user_feeds, user_conditions

def analyze_by_condition(uri_to_entities: Dict[str, List[Dict]], user_feeds: Dict, user_conditions: Dict) -> Dict:
    """
    Analyze top 10 entities by condition.
    """
    condition_entities = defaultdict(list)
    
    for user_id, feeds in user_feeds.items():
        condition = user_conditions[user_id]
        for feed_date, post_uris in feeds.items():
            for uri in post_uris:
                entities = uri_to_entities.get(uri, [])
                condition_entities[condition].extend(entities)
    
    # Count entities by condition
    condition_counts = {}
    for condition, entities in condition_entities.items():
        entity_counts = Counter()
        for entity in entities:
            entity_counts[entity["entity_normalized"]] += 1
        condition_counts[condition] = dict(entity_counts.most_common(10))
    
    return condition_counts

def analyze_by_pre_post_election(uri_to_entities: Dict[str, List[Dict]], user_feeds: Dict) -> Dict:
    """
    Analyze top 10 entities by pre-election vs post-election.
    Uses actual date comparison with cutoff date 2024-11-05.
    """
    cutoff_date = "2024-11-05"
    pre_election_entities = []
    post_election_entities = []
    
    for user_id, feeds in user_feeds.items():
        for feed_date, post_uris in feeds.items():
            for uri in post_uris:
                entities = uri_to_entities.get(uri, [])
                
                # Convert feed_date to datetime for proper comparison
                feed_datetime = datetime.strptime(feed_date, "%Y-%m-%d")
                cutoff_datetime = datetime.strptime(cutoff_date, "%Y-%m-%d")
                
                if feed_datetime <= cutoff_datetime:
                    pre_election_entities.extend(entities)
                else:
                    post_election_entities.extend(entities)
    
    # Count entities by time period
    pre_counts = Counter()
    post_counts = Counter()
    
    for entity in pre_election_entities:
        pre_counts[entity["entity_normalized"]] += 1
    
    for entity in post_election_entities:
        post_counts[entity["entity_normalized"]] += 1
    
    return {
        "pre_election": dict(pre_counts.most_common(10)),
        "post_election": dict(post_counts.most_common(10))
    }

def analyze_by_condition_and_pre_post(uri_to_entities: Dict[str, List[Dict]], user_feeds: Dict, user_conditions: Dict) -> Dict:
    """
    Analyze top 10 entities by condition x pre-election/post-election.
    Uses actual date comparison with cutoff date 2024-11-05.
    """
    cutoff_date = "2024-11-05"
    condition_time_entities = defaultdict(lambda: defaultdict(list))
    
    for user_id, feeds in user_feeds.items():
        condition = user_conditions[user_id]
        for feed_date, post_uris in feeds.items():
            # Convert feed_date to datetime for proper comparison
            feed_datetime = datetime.strptime(feed_date, "%Y-%m-%d")
            cutoff_datetime = datetime.strptime(cutoff_date, "%Y-%m-%d")
            
            time_period = "pre_election" if feed_datetime <= cutoff_datetime else "post_election"
            
            for uri in post_uris:
                entities = uri_to_entities.get(uri, [])
                condition_time_entities[condition][time_period].extend(entities)
    
    # Count entities by condition and time period
    results = {}
    for condition, time_data in condition_time_entities.items():
        results[condition] = {}
        for time_period, entities in time_data.items():
            entity_counts = Counter()
            for entity in entities:
                entity_counts[entity["entity_normalized"]] += 1
            results[condition][time_period] = dict(entity_counts.most_common(10))
    
    return results

def compare_pre_post_rankings(pre_post_data: Dict) -> Dict:
    """
    Compare rankings between pre-election and post-election periods.
    """
    pre_entities = list(pre_post_data["pre_election"].keys())
    post_entities = list(pre_post_data["post_election"].keys())
    
    comparison = {
        "pre_to_post": {},
        "post_to_pre": {}
    }
    
    # For each pre-election top 10 entity, find its rank in post-election
    for i, entity in enumerate(pre_entities):
        rank_in_pre = i + 1
        rank_in_post = post_entities.index(entity) + 1 if entity in post_entities else "Not in top 10"
        comparison["pre_to_post"][entity] = {
            "pre_rank": rank_in_pre,
            "post_rank": rank_in_post,
            "change": rank_in_pre - rank_in_post if isinstance(rank_in_post, int) else "N/A"
        }
    
    # For each post-election top 10 entity, find its rank in pre-election
    for i, entity in enumerate(post_entities):
        rank_in_post = i + 1
        rank_in_pre = pre_entities.index(entity) + 1 if entity in pre_entities else "Not in top 10"
        comparison["post_to_pre"][entity] = {
            "post_rank": rank_in_post,
            "pre_rank": rank_in_pre,
            "change": rank_in_post - rank_in_pre if isinstance(rank_in_pre, int) else "N/A"
        }
    
    return comparison

def compare_pre_post_by_condition(condition_time_data: Dict) -> Dict:
    """
    Compare pre/post rankings for each condition separately.
    """
    results = {}
    
    for condition, time_data in condition_time_data.items():
        pre_entities = list(time_data["pre_election"].keys())
        post_entities = list(time_data["post_election"].keys())
        
        comparison = {
            "pre_to_post": {},
            "post_to_pre": {}
        }
        
        # For each pre-election top 10 entity, find its rank in post-election
        for i, entity in enumerate(pre_entities):
            rank_in_pre = i + 1
            rank_in_post = post_entities.index(entity) + 1 if entity in post_entities else "Not in top 10"
            comparison["pre_to_post"][entity] = {
                "pre_rank": rank_in_pre,
                "post_rank": rank_in_post,
                "change": rank_in_pre - rank_in_post if isinstance(rank_in_post, int) else "N/A"
            }
        
        # For each post-election top 10 entity, find its rank in pre-election
        for i, entity in enumerate(post_entities):
            rank_in_post = i + 1
            rank_in_pre = pre_entities.index(entity) + 1 if entity in pre_entities else "Not in top 10"
            comparison["post_to_pre"][entity] = {
                "post_rank": rank_in_post,
                "pre_rank": rank_in_pre,
                "change": rank_in_post - rank_in_pre if isinstance(rank_in_pre, int) else "N/A"
            }
        
        results[condition] = comparison
    
    return results

def visualize_results(condition_analysis: Dict, pre_post_analysis: Dict, 
                     condition_time_analysis: Dict, ranking_comparison: Dict,
                     condition_ranking_comparison: Dict, output_dir: str = "ml_tooling/ner/experimental_results"):
    """
    Create comprehensive visualizations for NER analysis results.
    
    Args:
        condition_analysis: Top 10 entities by condition
        pre_post_analysis: Top 10 entities by pre/post-election
        condition_time_analysis: Top 10 entities by condition x pre/post-election
        ranking_comparison: Overall pre/post ranking comparison
        condition_ranking_comparison: Pre/post ranking comparison by condition
        output_dir: Base output directory
    """
    # Create timestamped directory structure
    timestamp = current_datetime_str
    base_dir = os.path.join(output_dir, timestamp)
    
    # Create directory structure
    directories = [
        os.path.join(base_dir, "condition"),
        os.path.join(base_dir, "election_date"),
        os.path.join(base_dir, "overall")
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    # Set up plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # 1. Top 10 entities by condition
    print("Creating condition-based visualizations...")
    create_condition_visualizations(condition_analysis, os.path.join(base_dir, "condition"))
    
    # 2. Pre/post-election visualizations
    print("Creating election date visualizations...")
    create_election_date_visualizations(pre_post_analysis, ranking_comparison, 
                                      condition_time_analysis, condition_ranking_comparison,
                                      os.path.join(base_dir, "election_date"))
    
    # 3. Overall visualizations
    print("Creating overall visualizations...")
    create_overall_visualizations(condition_analysis, pre_post_analysis, ranking_comparison,
                                os.path.join(base_dir, "overall"))
    
    # 4. Create visualization metadata
    create_visualization_metadata(base_dir, timestamp, condition_analysis, pre_post_analysis)
    
    print(f"All visualizations saved to: {base_dir}")
    return base_dir

def export_top_entities_to_csv(entities_dict: Dict[str, int], output_path: str, title: str):
    """Export top entities to CSV file sorted by count (descending)."""
    # Sort entities by count (descending)
    sorted_entities = sorted(entities_dict.items(), key=lambda x: x[1], reverse=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['term', 'count'])
        for entity, count in sorted_entities:
            writer.writerow([entity, count])

def create_condition_visualizations(condition_analysis: Dict, output_dir: str):
    """Create visualizations for condition-based analysis."""
    
    # Condition mapping for presentation
    condition_labels = {
        'reverse_chronological': 'Reverse Chronological',
        'engagement': 'Engagement-Based', 
        'representative_diversification': 'Diversified Extremity'
    }
    
    # Color palette
    condition_colors = {
        'reverse_chronological': 'grey',
        'engagement': 'red',
        'representative_diversification': 'green'
    }
    
    # Export CSV for each condition
    for condition, entities in condition_analysis.items():
        csv_path = os.path.join(output_dir, f"top_10_entities_{condition}.csv")
        export_top_entities_to_csv(entities, csv_path, f"Top Entities - {condition_labels[condition]}")
    
    # Create horizontal bar chart with top 10 entities per condition overlaid
    fig, ax = plt.subplots(figsize=(16, 12))
    
    conditions = list(condition_analysis.keys())
    
    # Get top 10 entities for each condition
    condition_top_entities = {}
    for condition, entities in condition_analysis.items():
        top_entities = dict(Counter(entities).most_common(10))
        condition_top_entities[condition] = top_entities
    
    # Create horizontal bars for each condition
    y_positions = []
    entity_labels = []
    bar_colors = []
    bar_width = 0.6
    
    # Calculate max frequency for scaling
    max_freq = max(max(entities.values()) if entities else 0 for entities in condition_top_entities.values())
    
    # Create bars for each condition
    for i, condition in enumerate(conditions):
        entities = condition_top_entities[condition]
        if entities:
            # Sort entities by frequency (descending)
            sorted_entities = sorted(entities.items(), key=lambda x: x[1], reverse=True)
            
            for j, (entity_name, frequency) in enumerate(sorted_entities):
                y_pos = i * 10 + j  # Offset each condition by 10 positions
                y_positions.append(y_pos)
                entity_labels.append(entity_name)
                bar_colors.append(condition_colors[condition])
                
                # Create horizontal bar
                ax.barh(y_pos, frequency, bar_width, color=condition_colors[condition], alpha=0.8)
                
                # Add entity name and frequency as text
                ax.text(frequency + max_freq * 0.02, y_pos, f"{entity_name} ({frequency})", 
                       va='center', fontsize=9, fontweight='bold')
    
    # Set y-axis labels and ticks
    ax.set_yticks(y_positions)
    ax.set_yticklabels(entity_labels)
    ax.set_xlabel('Frequency')
    ax.set_title('Top 10 Entities by Condition')
    ax.grid(True, alpha=0.3, axis='x')
    
    # Set x-axis limit with some padding
    ax.set_xlim(0, max_freq * 1.3)
    
    # Create legend
    legend_elements = []
    for condition in conditions:
        legend_elements.append(
            plt.Rectangle((0,0),1,1, facecolor=condition_colors[condition], alpha=0.8, 
                        label=condition_labels[condition])
        )
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "top_10_entities_by_condition.png"), 
                dpi=300, bbox_inches='tight')
    plt.close()
    
    # Create a separate detailed chart showing top 5 entities per condition
    fig, axes = plt.subplots(1, 3, figsize=(18, 8))
    
    for i, condition in enumerate(conditions):
        entities = condition_top_entities[condition]
        if entities:
            # Get top 5 entities for this condition
            top_5 = dict(list(entities.items())[:5])
            
            entity_names = list(top_5.keys())
            frequencies = list(top_5.values())
            
            # Create horizontal bar chart for this condition
            axes[i].barh(range(len(entity_names)), frequencies, color=condition_colors[condition], alpha=0.8)
            axes[i].set_yticks(range(len(entity_names)))
            axes[i].set_yticklabels(entity_names)
            axes[i].set_xlabel('Frequency')
            axes[i].set_title(f'Top 5 Entities - {condition_labels[condition]}')
            axes[i].grid(True, alpha=0.3, axis='x')
        else:
            axes[i].text(0.5, 0.5, 'No entities found', ha='center', va='center', transform=axes[i].transAxes)
            axes[i].set_title(f'Top 5 Entities - {condition_labels[condition]}')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "top_5_entities_by_condition_detailed.png"), 
                dpi=300, bbox_inches='tight')
    plt.close()

def create_election_date_visualizations(pre_post_analysis: Dict, ranking_comparison: Dict,
                                      condition_time_analysis: Dict, condition_ranking_comparison: Dict,
                                      output_dir: str):
    """Create visualizations for election date analysis."""
    
    # Export CSV files for pre/post election entities
    pre_csv_path = os.path.join(output_dir, "top_10_entities_pre_election.csv")
    export_top_entities_to_csv(pre_post_analysis["pre_election"], pre_csv_path, "Top Entities - Pre-Election")
    
    post_csv_path = os.path.join(output_dir, "top_10_entities_post_election.csv")
    export_top_entities_to_csv(pre_post_analysis["post_election"], post_csv_path, "Top Entities - Post-Election")
    
    # 1. Top 10 entities pre/post-election (side-by-side bar chart)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Pre-election
    pre_entities = list(pre_post_analysis["pre_election"].keys())
    pre_counts = list(pre_post_analysis["pre_election"].values())
    
    ax1.barh(range(len(pre_entities)), pre_counts, color='lightblue', alpha=0.8, edgecolor='navy')
    ax1.set_yticks(range(len(pre_entities)))
    ax1.set_yticklabels(pre_entities)
    ax1.set_xlabel('Frequency')
    ax1.set_title('Top 10 Entities - Pre-Election (≤ 2024-11-05)')
    ax1.grid(True, alpha=0.3)
    
    # Post-election
    post_entities = list(pre_post_analysis["post_election"].keys())
    post_counts = list(pre_post_analysis["post_election"].values())
    
    ax2.barh(range(len(post_entities)), post_counts, color='darkblue', alpha=0.8, edgecolor='navy')
    ax2.set_yticks(range(len(post_entities)))
    ax2.set_yticklabels(post_entities)
    ax2.set_xlabel('Frequency')
    ax2.set_title('Top 10 Entities - Post-Election (> 2024-11-05)')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "top_10_entities_pre_post_election.png"), 
                dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Rank change visualization (overall)
    create_rank_change_visualization(ranking_comparison, output_dir, "overall_rank_change")
    
    # 3. Rank change by condition
    create_rank_change_by_condition_visualizations(condition_ranking_comparison, output_dir)

def create_rank_change_visualization(ranking_comparison: Dict, output_dir: str, filename_prefix: str):
    """Create rank movement analysis visualization (tornado chart style)."""
    
    # Prepare data for rank movement analysis
    pre_to_post = ranking_comparison["pre_to_post"]
    post_to_pre = ranking_comparison["post_to_pre"]
    
    # Collect all entities with their rank changes
    entities_data = []
    
    # Process pre-to-post changes
    for entity, data in pre_to_post.items():
        if isinstance(data["change"], int):
            entities_data.append({
                'entity': entity,
                'change': data["change"],
                'pre_rank': data["pre_rank"],
                'post_rank': data["post_rank"],
                'status': 'improved' if data["change"] > 0 else 'declined' if data["change"] < 0 else 'unchanged'
            })
        else:  # N/A - entity disappeared
            entities_data.append({
                'entity': entity,
                'change': -20,  # Large negative for visualization
                'pre_rank': data["pre_rank"],
                'post_rank': 15,  # Off chart
                'status': 'dropped'
            })
    
    # Process post-to-pre changes (for new entities)
    for entity, data in post_to_pre.items():
        if isinstance(data["change"], int) and data["change"] > 0:
            # This entity appeared in post-election but not in pre-election top 10
            entities_data.append({
                'entity': entity,
                'change': 20,  # Large positive for visualization
                'pre_rank': 15,  # Off chart
                'post_rank': data["post_rank"],
                'status': 'new'
            })
    
    # Sort by magnitude of change (descending)
    entities_data.sort(key=lambda x: abs(x['change']), reverse=True)
    
    # Create tornado chart
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Separate entities by status
    improved_entities = [e for e in entities_data if e['status'] == 'improved']
    declined_entities = [e for e in entities_data if e['status'] == 'declined']
    dropped_entities = [e for e in entities_data if e['status'] == 'dropped']
    new_entities = [e for e in entities_data if e['status'] == 'new']
    unchanged_entities = [e for e in entities_data if e['status'] == 'unchanged']
    
    # Combine all entities for y-axis positioning
    all_entities = improved_entities + declined_entities + dropped_entities + new_entities + unchanged_entities
    y_positions = range(len(all_entities))
    
    # Create horizontal bars
    for i, entity_data in enumerate(all_entities):
        entity = entity_data['entity']
        change = entity_data['change']
        status = entity_data['status']
        
        if status == 'improved':
            # Bar extending to the right (positive) - light green
            ax.barh(i, change, color='lightgreen', alpha=0.8)
            ax.text(change + 0.5, i, f'+{change}', va='center', fontsize=9, fontweight='bold')
        elif status == 'declined':
            # Bar extending to the left (negative) - light red
            ax.barh(i, change, color='lightcoral', alpha=0.8)
            ax.text(change - 0.5, i, str(change), va='center', ha='right', fontsize=9, fontweight='bold')
        elif status == 'dropped':
            # Bar extending to the left (negative) - dark red
            ax.barh(i, change, color='darkred', alpha=0.8)
            ax.text(change - 0.5, i, 'Dropped', va='center', ha='right', fontsize=9, fontweight='bold')
        elif status == 'new':
            # Bar extending to the right (positive) - dark green
            ax.barh(i, change, color='darkgreen', alpha=0.8)
            ax.text(change + 0.5, i, 'New', va='center', fontsize=9, fontweight='bold')
        else:  # unchanged
            # Bar extending to the right (zero) - grey
            ax.barh(i, change, color='grey', alpha=0.8)
            ax.text(change + 0.5, i, 'No Change', va='center', fontsize=9, fontweight='bold')
    
    # Set y-axis labels
    ax.set_yticks(y_positions)
    ax.set_yticklabels([e['entity'] for e in all_entities])
    
    # Set x-axis
    ax.set_xlabel('Δ Rank (Positive = Improved)')
    ax.set_xlim(-25, 25)
    
    # Add vertical line at zero
    ax.axvline(x=0, color='black', linestyle='-', alpha=0.3)
    
    # Add title
    ax.set_title('Pre → Post Election Rank Movement Analysis\nRank Movement Magnitude', 
                fontsize=14, fontweight='bold', pad=20)
    
    # Add legend with more categories
    legend_elements = [
        plt.Rectangle((0,0),1,1, facecolor='lightgreen', alpha=0.8, label='Improved Rank'),
        plt.Rectangle((0,0),1,1, facecolor='lightcoral', alpha=0.8, label='Declined Rank'),
        plt.Rectangle((0,0),1,1, facecolor='darkgreen', alpha=0.8, label='New (Post Only)'),
        plt.Rectangle((0,0),1,1, facecolor='darkred', alpha=0.8, label='Dropped (Pre Only)'),
        plt.Rectangle((0,0),1,1, facecolor='grey', alpha=0.8, label='No Change')
    ]
    ax.legend(handles=legend_elements, loc='upper right')
    
    # Add grid
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"{filename_prefix}.png"), 
                dpi=300, bbox_inches='tight')
    plt.close()

def create_rank_change_by_condition_visualizations(condition_ranking_comparison: Dict, output_dir: str):
    """Create rank change visualizations by condition."""
    
    # Create by_condition subdirectory
    condition_dir = os.path.join(output_dir, "by_condition")
    os.makedirs(condition_dir, exist_ok=True)
    
    for condition, comparison in condition_ranking_comparison.items():
        create_rank_change_visualization(comparison, condition_dir, f"{condition}_rank_change")

def create_overall_visualizations(condition_analysis: Dict, pre_post_analysis: Dict, 
                                ranking_comparison: Dict, output_dir: str):
    """Create overall summary visualizations."""
    
    # Condition mapping for presentation
    condition_labels = {
        'reverse_chronological': 'Reverse Chronological',
        'engagement': 'Engagement-Based', 
        'representative_diversification': 'Diversified Extremity'
    }
    
    # Color palette
    condition_colors = {
        'reverse_chronological': 'grey',
        'engagement': 'red',
        'representative_diversification': 'green'
    }
    
    # 1. Summary dashboard
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Top entities across all conditions
    all_entities = Counter()
    for entities in condition_analysis.values():
        for entity, count in entities.items():
            all_entities[entity] += count
    
    top_overall = all_entities.most_common(10)
    entities, counts = zip(*top_overall)
    
    ax1.barh(range(len(entities)), counts, color='steelblue')
    ax1.set_yticks(range(len(entities)))
    ax1.set_yticklabels(entities)
    ax1.set_xlabel('Total Frequency')
    ax1.set_title('Top 10 Entities Overall')
    ax1.grid(True, alpha=0.3)
    
    # Pre vs Post comparison
    pre_counts = list(pre_post_analysis["pre_election"].values())
    post_counts = list(pre_post_analysis["post_election"].values())
    
    x = np.arange(len(pre_counts))
    width = 0.35
    
    ax2.bar(x - width/2, pre_counts, width, label='Pre-Election', color='lightblue', alpha=0.8, edgecolor='navy')
    ax2.bar(x + width/2, post_counts, width, label='Post-Election', color='darkblue', alpha=0.8, edgecolor='navy')
    
    ax2.set_xlabel('Rank')
    ax2.set_ylabel('Frequency')
    ax2.set_title('Pre vs Post-Election Entity Frequencies')
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"#{i+1}" for i in range(len(pre_counts))])
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Condition distribution with new colors
    condition_counts = [sum(entities.values()) for entities in condition_analysis.values()]
    condition_names = [condition_labels[cond] for cond in condition_analysis.keys()]
    condition_colors_list = [condition_colors[cond] for cond in condition_analysis.keys()]
    
    ax3.pie(condition_counts, labels=condition_names, colors=condition_colors_list, 
            autopct='%1.1f%%', startangle=90)
    ax3.set_title('Entity Distribution by Condition')
    
    # Rank change summary
    pre_to_post = ranking_comparison["pre_to_post"]
    improved = sum(1 for data in pre_to_post.values() if isinstance(data["change"], int) and data["change"] > 0)
    declined = sum(1 for data in pre_to_post.values() if isinstance(data["change"], int) and data["change"] < 0)
    unchanged = sum(1 for data in pre_to_post.values() if isinstance(data["change"], int) and data["change"] == 0)
    disappeared = sum(1 for data in pre_to_post.values() if data["change"] == "N/A")
    
    categories = ['Improved', 'Declined', 'Unchanged', 'Disappeared']
    values = [improved, declined, unchanged, disappeared]
    colors = ['green', 'red', 'blue', 'gray']
    
    ax4.bar(categories, values, color=colors)
    ax4.set_ylabel('Number of Entities')
    ax4.set_title('Rank Change Summary (Pre → Post)')
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "overall_summary_dashboard.png"), 
                dpi=300, bbox_inches='tight')
    plt.close()

def create_visualization_metadata(base_dir: str, timestamp: str, condition_analysis: Dict, pre_post_analysis: Dict):
    """Create visualization metadata JSON file."""
    
    metadata = {
        "visualization_type": "named_entity_recognition",
        "creation_timestamp": timestamp,
        "election_date": "2024-11-05",
        "total_conditions": len(condition_analysis),
        "conditions": list(condition_analysis.keys()),
        "top_10_entities_by_condition": {
            condition: list(entities.keys()) for condition, entities in condition_analysis.items()
        },
        "top_10_entities_pre_election": list(pre_post_analysis["pre_election"].keys()),
        "top_10_entities_post_election": list(pre_post_analysis["post_election"].keys()),
        "figure_parameters": {
            "figsize": [12, 8],
            "dpi": 300,
            "facecolor": "white"
        },
        "color_palette": [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
        ],
        "note": "NER analysis with spaCy en_core_web_sm model focusing on PERSON, ORG, GPE, DATE entities"
    }
    
    metadata_path = os.path.join(base_dir, "visualization_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Visualization metadata saved to: {metadata_path}")

def demonstrate_ner_pipeline():
    """
    Demonstrate comprehensive NER analysis with 6 users across 3 conditions.
    """
    print("=" * 80)
    print("COMPREHENSIVE NER ANALYSIS - MET-52 Implementation Demo")
    print("=" * 80)
    
    # Step 1: Create user feeds and conditions setup
    print("\n1. SETTING UP USER FEEDS AND CONDITIONS")
    print("-" * 40)
    user_feeds, user_conditions = create_user_feeds()
    
    print(f"Created {len(user_feeds)} users across 3 conditions:")
    for user_id, condition in user_conditions.items():
        print(f"  {user_id}: {condition}")
        for feed_date, post_uris in user_feeds[user_id].items():
            print(f"    {feed_date}: {len(post_uris)} posts")
    
    # Step 2: Extract entities using spaCy
    print("\n2. EXTRACTING ENTITIES WITH SPACY")
    print("-" * 40)
    uri_to_entities = extract_entities_with_spacy(posts)
    total_entities = sum(len(entities) for entities in uri_to_entities.values())
    print(f"Extracted {total_entities} entities from {len(posts)} posts")
    
    # Step 3: Analysis by condition
    print("\n3. TOP 10 ENTITIES BY CONDITION")
    print("-" * 40)
    condition_analysis = analyze_by_condition(uri_to_entities, user_feeds, user_conditions)
    for condition, entities in condition_analysis.items():
        print(f"\n{condition.upper()}:")
        for i, (entity, count) in enumerate(entities.items(), 1):
            print(f"  {i:2d}. {entity}: {count}")
    
    # Step 4: Analysis by pre/post-election
    print("\n4. TOP 10 ENTITIES BY PRE/POST-ELECTION")
    print("-" * 40)
    pre_post_analysis = analyze_by_pre_post_election(uri_to_entities, user_feeds)
    
    print("\nPRE-ELECTION (≤ 2024-11-05):")
    for i, (entity, count) in enumerate(pre_post_analysis["pre_election"].items(), 1):
        print(f"  {i:2d}. {entity}: {count}")
    
    print("\nPOST-ELECTION (> 2024-11-05):")
    for i, (entity, count) in enumerate(pre_post_analysis["post_election"].items(), 1):
        print(f"  {i:2d}. {entity}: {count}")
    
    # Step 5: Pre/post ranking comparison
    print("\n5. PRE/POST-ELECTION RANKING COMPARISON")
    print("-" * 40)
    ranking_comparison = compare_pre_post_rankings(pre_post_analysis)
    
    print("\nPRE-ELECTION TOP 10 → POST-ELECTION RANKINGS:")
    for entity, data in ranking_comparison["pre_to_post"].items():
        print(f"  {entity}: Pre #{data['pre_rank']} → Post #{data['post_rank']} (Change: {data['change']})")
    
    print("\nPOST-ELECTION TOP 10 → PRE-ELECTION RANKINGS:")
    for entity, data in ranking_comparison["post_to_pre"].items():
        print(f"  {entity}: Post #{data['post_rank']} → Pre #{data['pre_rank']} (Change: {data['change']})")
    
    # Step 6: Analysis by condition x pre/post-election
    print("\n6. TOP 10 ENTITIES BY CONDITION × PRE/POST-ELECTION")
    print("-" * 40)
    condition_time_analysis = analyze_by_condition_and_pre_post(uri_to_entities, user_feeds, user_conditions)
    
    for condition, time_data in condition_time_analysis.items():
        print(f"\n{condition.upper()}:")
        print("  PRE-ELECTION:")
        for i, (entity, count) in enumerate(time_data["pre_election"].items(), 1):
            print(f"    {i:2d}. {entity}: {count}")
        print("  POST-ELECTION:")
        for i, (entity, count) in enumerate(time_data["post_election"].items(), 1):
            print(f"    {i:2d}. {entity}: {count}")
    
    # Step 7: Pre/post ranking comparison by condition
    print("\n7. PRE/POST-ELECTION RANKING COMPARISON BY CONDITION")
    print("-" * 40)
    condition_ranking_comparison = compare_pre_post_by_condition(condition_time_analysis)
    
    for condition, comparison in condition_ranking_comparison.items():
        print(f"\n{condition.upper()}:")
        print("  PRE-ELECTION TOP 10 → POST-ELECTION RANKINGS:")
        for entity, data in comparison["pre_to_post"].items():
            print(f"    {entity}: Pre #{data['pre_rank']} → Post #{data['post_rank']} (Change: {data['change']})")
        print("  POST-ELECTION TOP 10 → PRE-ELECTION RANKINGS:")
        for entity, data in comparison["post_to_pre"].items():
            print(f"    {entity}: Post #{data['post_rank']} → Pre #{data['pre_rank']} (Change: {data['change']})")
    
    # Step 8: Create visualizations
    print("\n8. CREATING VISUALIZATIONS")
    print("-" * 40)
    output_dir = visualize_results(
        condition_analysis, 
        pre_post_analysis, 
        condition_time_analysis, 
        ranking_comparison, 
        condition_ranking_comparison
    )
    
    # Step 9: Summary statistics
    print("\n9. SUMMARY STATISTICS")
    print("-" * 40)
    total_posts_analyzed = sum(len(post_uris) for feeds in user_feeds.values() for post_uris in feeds.values())
    print(f"Total posts analyzed: {total_posts_analyzed}")
    print(f"Total entities extracted: {total_entities}")
    print(f"Users per condition: {len(user_feeds) // 3}")
    print(f"Feeds per user: {len(list(user_feeds.values())[0])}")
    print(f"Posts per feed: {len(list(list(user_feeds.values())[0].values())[0])}")
    print(f"Visualizations saved to: {output_dir}")
    
    print("\n" + "=" * 80)
    print("COMPREHENSIVE NER ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    demonstrate_ner_pipeline()
