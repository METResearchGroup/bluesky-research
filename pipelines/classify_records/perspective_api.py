# 

# TODO: I should see if there's something in the Google API or the Jigsaw API
# that allows me to batch multiple requests together and have them sent
# together so I don't have to do that batching manually with async

def load_latest_label_timestamp() -> str:
    # need to think about what this would look like
    # also I should see if in the preprocessing pipeline if this is implemented
    # in the database file or in the helper file.
    pass


def classify_latest_posts():
    # load posts
    # classify
    # write to db
    pass
