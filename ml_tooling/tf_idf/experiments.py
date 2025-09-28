"""Experimentation file to help me better understand TF-IDF under the hood."""

from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

posts = [
    "Election officials report high turnout in Arizona.",
    "Turnout was lower in some counties but results are still coming in.",
    "Officials in Georgia discuss ballot counting and certification.",
    "Media outlets report results and projections after the election.",
    "County offices share updates on vote tabulation.",
    "Campaign rally highlights policy and jobs.",
    "Debate clips trend as candidates spar over the economy.",
    "Certification timelines differ across states and counties.",
    "Security and administration issues raised by observers.",
    "Local reporters cover results and recount discussions."
]

def l2_normalize_rows(X):
    """L2-normalize each row vector in a dense matrix X."""
    norms = np.sqrt((X * X).sum(axis=1, keepdims=True))
    norms[norms == 0] = 1.0
    return X / norms

def manual_tfidf(posts, vectorizer):
    """
    Compute TF-IDF manually using the SAME tokenization & stopwords as the fitted vectorizer.
    Returns:
      X_manual  : [n_docs, n_terms] dense numpy array (float64)
      terms     : feature names array ordered to match sklearn's matrix columns
      idf       : idf array aligned with terms
    """
    N = len(posts)
    analyzer = vectorizer.build_analyzer()  # same preprocessing + tokenization + stopwords
    vocab = {}  # term -> col idx (we’ll populate in SAME order sklearn uses)

    # First pass: use sklearn's fitted vocabulary so column ordering matches exactly
    # (We fit the vectorizer below—this is just planning ahead.)
    # We'll fill counts using that exact mapping.
    # To avoid refitting twice, we’ll fit once now to get the vocabulary, then ignore its transform.
    vectorizer.fit(posts)
    vocab = vectorizer.vocabulary_.copy()                 # {'term': index, ...}
    terms = vectorizer.get_feature_names_out()            # column names in index order
    V = len(terms)

    # vocab is a dictionary that maps a term to its index in the vocabulary.
    # terms is an array of the terms in the vocabulary, in the order they were
    # added to the vocabulary.
    # V is the number of terms in the vocabulary.
    """
    (Pdb) vocab
    {'election': 17, 'officials': 28, 'report': 35, 'high': 19, 'turnout': 45, 'arizona': 1,
    'lower': 24, 'counties': 8, 'results': 37, 'coming': 7, 'georgia': 18, 'discuss': 14, 'ballot': 2,
    'counting': 9, 'certification': 5, 'media': 25, 'outlets': 29, 'projections': 31, 'county': 10,
    'offices': 27, 'share': 39, 'updates': 46, 'vote': 47, 'tabulation': 42, 'campaign': 3,
    'rally': 33, 'highlights': 20, 'policy': 30, 'jobs': 22, 'debate': 12, 'clips': 6,
    'trend': 44, 'candidates': 4, 'spar': 40, 'economy': 16, 'timelines': 43,
    'differ': 13, 'states': 41, 'security': 38, 'administration': 0, 'issues': 21,
    'raised': 32, 'observers': 26, 'local': 23, 'reporters': 36, 'cover': 11, 'recount': 34,
    'discussions': 15}

    (Pdb) terms
    array(['administration', 'arizona', 'ballot', 'campaign', 'candidates',
       'certification', 'clips', 'coming', 'counties', 'counting',
       'county', 'cover', 'debate', 'differ', 'discuss', 'discussions',
       'economy', 'election', 'georgia', 'high', 'highlights', 'issues',
       'jobs', 'local', 'lower', 'media', 'observers', 'offices',
       'officials', 'outlets', 'policy', 'projections', 'raised', 'rally',
       'recount', 'report', 'reporters', 'results', 'security', 'share',
       'spar', 'states', 'tabulation', 'timelines', 'trend', 'turnout',
       'updates', 'vote'], dtype=object)

    (Pdb) V
    48
    """

    # Build term counts per doc aligned to vocab indices
    counts_per_doc = [dict() for _ in range(N)]
    for i, text in enumerate(posts):
        tokens = analyzer(text)
        # Count frequencies only for tokens in vocab
        for tok in tokens:
            j = vocab.get(tok)
            if j is not None:
                counts_per_doc[i][j] = counts_per_doc[i].get(j, 0) + 1

    # counts_per_doc is a list of dictionaries, one for each post, and each
    # dictionary contains a key = index of the term in the vocabulary, and a
    # value = frequency of the term in the post.
    """
    (Pdb) len(counts_per_doc)
    10
    (Pdb) counts_per_doc[0]
    {17: 1, 28: 1, 35: 1, 19: 1, 45: 1, 1: 1}
    (Pdb) counts_per_doc[1]
    {45: 1, 24: 1, 8: 1, 37: 1, 7: 1}
    """

    # Document frequency df[j] = number of docs containing term j
    df = np.zeros(V, dtype=np.int64)
    for d in counts_per_doc:
        for j in d.keys():
            df[j] += 1

    # df is a numpy array of shape (V,) where df[j] = number of docs containing term j
    """
    (Pdb) df
    array([1, 1, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 2, 1, 3, 1, 1, 1, 1, 1, 1,
        1, 2, 1, 1])
    (Pdb) df.shape
    (48,)
    """

    # Smooth IDF (sklearn): idf = ln( (1 + N) / (1 + df) ) + 1
    idf = np.log((1.0 + N) / (1.0 + df)) + 1.0

    # We want a (smoothed) inverse DF, hence the idf calculation and then + 1.
    """
    (Pdb) idf
    array([2.70474809, 2.70474809, 2.70474809, 2.70474809, 2.70474809,
        2.29928298, 2.70474809, 2.70474809, 2.29928298, 2.70474809,
        2.70474809, 2.70474809, 2.70474809, 2.70474809, 2.70474809,
        2.70474809, 2.70474809, 2.29928298, 2.70474809, 2.70474809,
        2.70474809, 2.70474809, 2.70474809, 2.70474809, 2.70474809,
        2.70474809, 2.70474809, 2.70474809, 2.29928298, 2.70474809,
        2.70474809, 2.70474809, 2.70474809, 2.70474809, 2.70474809,
        2.29928298, 2.70474809, 2.01160091, 2.70474809, 2.70474809,
        2.70474809, 2.70474809, 2.70474809, 2.70474809, 2.70474809,
        2.29928298, 2.70474809, 2.70474809])
    """

    # Build raw TF-IDF (no normalization yet): tf * idf
    X = np.zeros((N, V), dtype=np.float64)
    for i, d in enumerate(counts_per_doc):
        if not d:
            continue
        for j, tf in d.items():
            # X[document index, term index] = (how often the term appears in the document) * (how often the term appears in the corpus)
            X[i, j] = tf * idf[j]

    """
    (Pdb) X.shape
    (10, 48)
    """

    # L2 normalize rows to match TfidfVectorizer(norm='l2')
    X = l2_normalize_rows(X)

    return X, terms, idf

def top_k_for_doc(row, terms, k=3):
    """Return top-k (term, weight) from a dense 1xV row."""
    nz = np.flatnonzero(row)
    if nz.size == 0:
        return []
    sub = row[nz]
    order = nz[np.argsort(-sub)[:k]]
    return [(terms[j], float(row[j])) for j in order]

if __name__ == "__main__":
    # ------------- Compute manual TF-IDF -------------
    # Build a *fresh* vectorizer with the exact defaults we want to replicate
    vec = TfidfVectorizer(
        lowercase=True,
        stop_words="english",
        ngram_range=(1,1),      # unigrams only
        use_idf=True,
        smooth_idf=True,
        sublinear_tf=False,
        norm="l2"
    )
    X_manual, terms, idf = manual_tfidf(posts, vec)

    # ------------- Compute sklearn TF-IDF (for comparison) -------------
    X_sklearn = vec.fit_transform(posts).toarray()
    terms_sklearn = vec.get_feature_names_out()

    # Sanity check: feature ordering should match
    assert np.array_equal(terms, terms_sklearn), "Feature names/order mismatch with sklearn!"

    # Compare matrices
    max_abs_diff = np.max(np.abs(X_manual - X_sklearn))
    print(f"Max |manual - sklearn| difference: {max_abs_diff:.10f}  (should be ~0)")

    # ------------- Mean TF-IDF overall (manual) -------------
    overall = X_manual.mean(axis=0)  # average across documents
    top_idx = np.argsort(-overall)[:10]
    print("\nTop 10 terms overall (by mean TF-IDF) — MANUAL:")
    for j in top_idx:
        print(f"{terms[j]:20s} {overall[j]:.4f}")

    # ------------- Top-3 per document (manual) -------------
    print("\nTop 3 terms per post — MANUAL:")
    for i in range(X_manual.shape[0]):
        tops = top_k_for_doc(X_manual[i], terms, k=3)
        pretty = ", ".join([f"{t}({v:.3f})" for t, v in tops])
        print(f"Doc {i:02d}: {pretty}")
