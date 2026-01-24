def calculate_accuracy(ground_truth_labels: list[int], labels: list[int]) -> float:
    """Calculates accuracy of labels compared to ground truth labels.
    
    Returns:
        float: Accuracy of labels compared to ground truth labels.
    """
    correct = 0
    for ground_truth_label, label in zip(ground_truth_labels, labels):
        if ground_truth_label == label:
            correct += 1
    return correct / len(ground_truth_labels)
