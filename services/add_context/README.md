# Contextualization

This service adds context for a given post. The text of many posts only make sense when provided the appropriate context. For example, a post may be reacting to a wider thread or a post may have an associated image that the author is reacting to. We'll enrich each post with the associated context for the post.

#### 1. Enriching our classifiers with knowledge of current events
Many posts make reference to entities such as politician names, legislature, and locations, which are only civic in the context of current events. However, any classifier or language model will lack that context. To enrich our models with knowledge of current events, we will, once a day, download the top articles from the New York Times, via their developer API, and use a combination of machine learning techniques to extract the keywords relating to current news. We will store these keywords into a database. We will then note which posts contain any of these keywords pertaining to current events and add that as a feature to our models.

#### 2. Pulling the headers of external articles
Many posts link to an external article. We will extract the title of any articles linked to a given post.

#### 3. Extracting text within images via OCR
Many posts reference an image such as a screenshot of a news article or a post or tweet. We will perform OCR in order to extract the text embedded within any image.

#### 4. Referencing the parent post for a comment
Many posts either react to another post or are a comment to another post, and only make sense in the context of that post. We will link a given post to the parent post or thread that it is responding to, if applicable.