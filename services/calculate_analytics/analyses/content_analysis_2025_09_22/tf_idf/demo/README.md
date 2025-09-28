# TF-IDF Demo

This demo demonstrates TF-IDF analysis on simulated social media posts with multiple sources and time periods, showing how to train models and create various visualizations. The demo generates general content without topic-specific constraints.

## Scenario

- **3 Sources**: A, B, C (general content, no topic constraints)
- **2 Time Periods**: pre, post
- **6 Total Groups**: A_pre, A_post, B_pre, B_post, C_pre, C_post
- **100 posts per group** (600 total posts)

## Files

- `tfidf_demo_train.py` - Training script that generates fake data and trains TF-IDF models
- `tfidf_demo_visualize.py` - Visualization script that creates charts from training results
- `requirements.txt` - Python package dependencies
- `README.md` - This file

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run training**:
   ```bash
   python tfidf_demo_train.py
   ```

3. **Create visualizations**:
   ```bash
   python tfidf_demo_visualize.py
   ```

4. **Check results**:
   - Training results: `demo_results/`
   - Visualizations: `demo_results/visualizations/`

## What the Demo Shows

### TF-IDF Training Approaches

1. **Global TF-IDF**: Train on entire corpus (all 600 posts)
   - Pros: Consistent vocabulary, better for comparison
   - Cons: May miss group-specific terms

2. **Group-Specific TF-IDF**: Train separate models for each group
   - Pros: Captures group-specific vocabulary
   - Cons: Different vocabularies make comparison harder

3. **Stratified Analysis**: Use global model but analyze by subgroups
   - Best of both worlds: consistent vocabulary + group analysis

### Visualizations Created

1. **Top Keywords Bar Chart** - Horizontal bar chart showing highest TF-IDF scores
2. **Source Comparison** - Side-by-side comparison of keywords across sources A, B, C
3. **Period Comparison** - Pre vs post period keyword analysis
4. **Cross-Dimensional Heatmap** - Heatmap showing keyword intensity across source-period combinations
5. **Individual Analysis** - Separate charts for each source and period

## Key TF-IDF Concepts Demonstrated

### What is TF-IDF?

**TF-IDF** (Term Frequency-Inverse Document Frequency) measures how important a word is to a document in a collection of documents.

- **TF (Term Frequency)**: How often a term appears in a document
- **IDF (Inverse Document Frequency)**: How rare a term is across all documents
- **TF-IDF Score**: TF × IDF (higher = more important/unique)

### Why Use TF-IDF?

- **Keyword Extraction**: Find the most important words in text
- **Text Similarity**: Compare documents based on important terms
- **Feature Engineering**: Convert text to numerical features for ML
- **Content Analysis**: Understand what topics are being discussed

### TF-IDF Variations

1. **N-grams**: Use word pairs (bigrams) or triplets (trigrams)
   - Example: "machine learning" as one feature instead of separate "machine" and "learning"

2. **Min/Max Document Frequency**: Filter out very rare or very common terms
   - `min_df=2`: Ignore terms appearing in < 2 documents
   - `max_df=0.95`: Ignore terms appearing in > 95% of documents

3. **Stop Words**: Remove common words like "the", "and", "is"
   - `stop_words='english'`: Remove English stop words

4. **Max Features**: Limit vocabulary size for performance
   - `max_features=1000`: Keep only top 1000 most frequent terms

## Output Files

### Training Results (`demo_results/`)
- `dataset.csv` - Original dataset with posts and labels
- `global_vectorizer.pkl` - Trained global TF-IDF model
- `group_models.pkl` - Group-specific TF-IDF models
- `*_keywords.csv` - Top keywords for each analysis type
- `metadata.json` - Training configuration and statistics

### Visualizations (`demo_results/visualizations/`)
- `overall_top_keywords.png` - Top 15 keywords overall
- `source_comparison.png` - Keywords by source (A, B, C)
- `period_comparison.png` - Keywords by period (pre, post)
- `cross_dimensional_heatmap.png` - Source × period heatmap
- `source_*_keywords.png` - Individual source analysis
- `period_*_keywords.png` - Individual period analysis
- `visualization_metadata.json` - Visualization configuration

## Customization

### Modify the Demo

1. **Change number of posts**: Edit `N_POSTS_PER_GROUP` in `tfidf_demo_train.py`
2. **Add more sources**: Modify `SOURCES` list
3. **Change time periods**: Update `TIME_PERIODS` list
4. **Adjust TF-IDF parameters**: Modify `TfidfVectorizer` settings
5. **Add new visualizations**: Create new functions in `tfidf_demo_visualize.py`

### Scale to Real Data

1. **Replace fake data generation** with real data loading
2. **Increase vocabulary size** (`max_features`)
3. **Add more sophisticated preprocessing** (stemming, lemmatization)
4. **Implement memory-efficient processing** for large datasets
5. **Add parallel processing** for multiple groups

## Performance Notes

- **Memory**: TF-IDF matrices can be large (documents × features)
- **Speed**: Group-specific models are slower but more accurate
- **Storage**: Pickle files can be large for big vocabularies
- **Visualization**: Many charts can take time to render

## Next Steps

1. **Try different parameters**: Experiment with n-gram ranges, min/max DF
2. **Add preprocessing**: Stemming, lemmatization, custom tokenization
3. **Implement clustering**: Use TF-IDF features for document clustering
4. **Add classification**: Train classifiers on TF-IDF features
5. **Scale up**: Test with larger datasets and more groups

## Troubleshooting

- **Import errors**: Make sure all packages are installed (`pip install -r requirements.txt`)
- **Memory issues**: Reduce `max_features` or `N_POSTS_PER_GROUP`
- **Empty results**: Check that data generation is working correctly
- **Visualization errors**: Ensure matplotlib backend is properly configured
