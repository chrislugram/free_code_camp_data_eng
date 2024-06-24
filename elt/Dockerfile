# Use the official Python image as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock ./

# Install Poetry and its dependencies
RUN pip install poetry
RUN poetry install --no-root --no-dev

# Copy the rest of the application code into the container
COPY . .

# Run the application
CMD ["poetry", "run", "python", "main.py"]