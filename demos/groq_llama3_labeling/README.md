# Labeling posts with Llama3-8b via Groq

We want to figure out what model would be good for our inference task. We've been experimenting with Llama3-8b via Groq and it seems to perform pretty well for our zero-shot task! Let's see how this performs on our pilot data.

We'll use Groq for this, and run it on 300 posts from our pilot data. We'll use the same prompts that we did from our demo Streamlit app.

We'll use the LiteLLM [Groq](https://litellm.vercel.app/docs/providers/groq) connection to connect to Groq.
