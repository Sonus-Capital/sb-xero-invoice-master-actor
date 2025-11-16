FROM apify/actor-python:3.13

# Copy the whole repo (including .actor/src/)
COPY . ./

# Optional: install requirements if you later add requirements.txt
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Run our main script
CMD ["python", ".actor/src/main.py"]
