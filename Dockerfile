# Stage 1: Build Stage
FROM python:3.10.12-slim AS build
# Install git and build dependencies, then clean up the apt cache
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*
# Set the working directory
WORKDIR /app
# Copy the requirements file first to leverage Docker cache
COPY requirements.txt .
# Install dependencies
RUN python3 -m venv /venv \
    && . /venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip cache purge  # Clean up pip cache to reduce image size
# Copy the source code into the container
COPY . .
# Stage 2: Final Stage
FROM python:3.10.12-slim
# Set the working directory
WORKDIR /app
# Copy the virtual environment from the build stage
COPY --from=build /venv /venv
# Copy only the necessary files to the final image
COPY --from=build /app /app
# Set Python environment variable for venv path
ENV PATH="/venv/bin:$PATH"
# Expose the port the app runs on
EXPOSE 8015
# Start the app
CMD ["python3", "RefactorApp.py"]