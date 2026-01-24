# can basically increase this as much as we want,
# as LiteLLM handles underlying concurrency management,
# though it's around this point that we start seeing
# linear to sublinear performance improvements
# https://github.com/METResearchGroup/bluesky-research/pull/369
DEFAULT_BATCH_SIZE = 500
DEFAULT_LLM_MODEL_NAME = "gpt-5-nano"
