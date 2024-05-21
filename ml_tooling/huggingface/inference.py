# Use a pipeline as a high-level helper
from transformers import pipeline

pipe = pipeline("text-generation", model="unsloth/llama-3-8b")
result = pipe("I am a highly skilled", max_length=50)

if __name__ == "__main__":
    print(result)
