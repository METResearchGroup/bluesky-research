from services.cluster_candidates.main import cluster_candidates  # noqa
from services.train_candidate_generation.main import train_candidate_generation_model  # noqa


def run_candidate_generation(retrain_candidate_generation_model: bool = False):
    """Generates candidates for users.

    Optionally runs the candidate generation pipeline as well in order
    to refresh the candidates.
    """
    if retrain_candidate_generation_model:
        train_candidate_generation_model()
        cluster_candidates()
