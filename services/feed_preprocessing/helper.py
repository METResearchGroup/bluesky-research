"""Helper functions for feed preprocessing."""
import os

from lib.aws.s3 import S3, S3_FIREHOSE_KEY_ROOT
from services.feed_preprocessing.pipelines import (
    feed_preprocessing_pipeline, filtering_pipeline
)

PREPROCESSED_DATA_KEY_ROOT = "preprocessed_data"

class DataLoader:
    """Loads raw data from S3 in batches, then yields them to the lambda
    so that they can be processed incrementally without having to be all
    loaded into memory.

    Note: we already save the raw data in S3 in batches. The "chunk_size"
    parameter is used to determine how many files to load at a time.

    For example, if we have ~100 posts per .jsonl file and we set the chunk_size
    to 5, then we would load 500 posts at a time.

    This shouldn't be an issue since the posts take up very little memory. For example,
    100 posts ~ 3KB. Depending on the lambda size, the memory allocated to a
    lambda is 128MB - 3GB. So we can load a lot of posts at once.
    """ # noqa

    def __init__(self, chunk_size: int = 5) -> None:
        self.raw_keys: list[str] = self.get_all_raw_data_keys()
        self.chunk_size: int = chunk_size
        self.index: int = 0
        self.s3: S3 = S3()

    def get_all_raw_data_keys(
        self, root_key: str = S3_FIREHOSE_KEY_ROOT
    ) -> list[str]:
        """Gets the keys of all the raw data."""
        return self.s3.get_keys_given_prefix(prefix=root_key)

    def load_next_batch(self) -> list[tuple[str, list[dict]]]:
        """Loads each batch in a list of tuples of (key, posts)"""
        jsonl_files_to_load = self.raw_keys[
            self.index:self.index + self.chunk_size
        ]
        self.index += self.chunk_size
        res = []
        for key in jsonl_files_to_load:
            posts = self.s3.read_jsonl_from_s3(key=key)
            res.append((key, posts))
        return res

    # TODO: the posts should be written to a folder corresponding to the
    # timestamp of this batch preprocessing being done. This way, we can
    # keep track of when the preprocessing was done and be able to pull
    # only the most recent posts.
    def write_preprocessed_post_to_s3(self, post: dict) -> None:
        """Writes preprocessed post to S3.

        Writes to a new "preprocessed" directory.
        """
        hashed_filename = f"{hash(post['uri'])}.json"
        key = os.path.join(PREPROCESSED_DATA_KEY_ROOT, hashed_filename)
        self.s3.write_dict_json_to_s3(data=post, key=key)

    def preprocess_batch(
        self,
        batch: list[tuple[str, list[dict]]],
        filtering_pipeline: list[callable],
        preprocessing_pipeline: list[callable]
    ) -> None:
        """Perform batch preprocessing on the data and then writes them back
        to S3."""
        total_batch_posts_preprocessed = 0
        for (key, posts) in batch:
            for post in posts:
                # pass post through all filters concurrently. If any filter
                # returns False, then the post is filtered out.
                post_passes_filtering = all(
                    [filter_func(post) for filter_func in filtering_pipeline]
                )
                # if post passes filtering, do necessary preprocessing and then
                # write to s3
                if post_passes_filtering:
                    for func in preprocessing_pipeline:
                        post = func(post)
                    self.write_preprocessed_post_to_s3(post)
                    total_batch_posts_preprocessed += 1
            # remove the raw data from S3.
            self.s3.delete_from_s3(key=key)
        print(f"Preprocessed {total_batch_posts_preprocessed} posts in this batch.") # noqa

    def preprocess_batches(
        self,
        filtering_pipeline: list[callable],
        preprocessing_pipeline: list[callable]
    ) -> None:
        """Perform preprocessing on the data in batches, and then writes
        them back to S3."""
        while True:
            batch = self.load_next_batch()
            if not batch:
                break
            self.preprocess_batch(
                batch=batch,
                filtering_pipeline=filtering_pipeline,
                preprocessing_pipeline=preprocessing_pipeline
            )


def preprocess_raw_data(chunk_size: int = 5) -> None:
    """Preprocesses raw data from S3 and writes the processed data back to S3.""" # noqa
    dataloader = DataLoader(chunk_size=chunk_size)
    dataloader.preprocess_batches(
        filtering_pipeline=filtering_pipeline,
        preprocessing_pipeline=feed_preprocessing_pipeline
    )
