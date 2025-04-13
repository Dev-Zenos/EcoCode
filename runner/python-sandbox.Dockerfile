# Use an official slim Python image as a base
FROM python:3.10-slim

# Set a working directory inside the container
WORKDIR /app

# --- Security Best Practice: Create a non-root user ---
# Create a group and user with no home directory and no login shell
# Using numeric UID/GID > 1000 is often recommended to avoid conflicts
RUN groupadd -r -g 1001 appgroup && useradd --no-log-init -r -u 1001 -g appgroup appuser

# --- Switch to the non-root user ---
# Any subsequent RUN, CMD, ENTRYPOINT will be executed as this user
USER appuser

# You could pre-install common libraries here if needed, e.g.:
# RUN pip install --no-cache-dir numpy requests

# Default command (optional, as we override it in docker run)
# CMD ["python"]