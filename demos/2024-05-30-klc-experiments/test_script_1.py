"""First test.

Intended to rerun the same test script as in the "Slurm basics" repo.

Reference: https://github.com/rs-kellogg/krs-openllm-cookbook/blob/main/scripts/slurm_basics/pytorch_gpu_test.py
"""  # noqa
import time

import torch

start_time = time.time()

# Check if CUDA is available
if torch.cuda.is_available():
    print("CUDA is available!")
    print("Number of GPUs available:", torch.cuda.device_count())
    print("GPU:", torch.cuda.get_device_name(0))
else:
    print("CUDA is not available.")

# Check if CUDA is available and set the device accordingly
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Print whether a GPU or CPU is being used
if device.type == 'cuda':
    print("Using GPU")
else:
    print("Using CPU")

# Create two random tensors
tensor1 = torch.randn(1000, 1000, device=device)
tensor2 = torch.randn(1000, 1000, device=device)

# Add the two tensors, the operation will be performed on the GPU if available
result = tensor1 + tensor2

print(result)
end_time = time.time()
total_runtime = end_time - start_time
print(f"Total runtime: {total_runtime:.2f} seconds.")
