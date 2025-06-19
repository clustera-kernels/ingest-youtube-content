FROM python:3.12-slim

WORKDIR /app

# Copy requirements and readme
COPY pyproject.toml README.md ./

# Install uv for faster dependency management
RUN pip install uv

# Install dependencies
RUN uv sync

# Copy application code
COPY . .

# Command to run the application using uv
CMD ["uv", "run", "python", "main.py"]