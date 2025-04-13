# Use an official slim Python image as a base
FROM python:3.10-slim

# Set a working directory inside the container
WORKDIR /app

# --- Dependency Installation ---
# Copy the requirements file for the USER'S code FIRST
# Assume the user code (mounted later) includes a requirements.txt
# We copy a placeholder or common requirements first if needed,
# OR rely on the mounted volume having it. For robustness,
# let's assume the user provides it in their code directory.
# The COPY command here might not be strictly necessary if requirements.txt
# is ONLY in the user's code dir, but shown for clarity.
# COPY requirements.txt /app/

# Install dependencies specified by the user's code
# This command will run when the image is BUILT.
# If requirements can change often, consider installing them at runtime
# or rebuilding the image frequently. A common pattern is:
    COPY requirements.txt /app/
    RUN pip install --no-cache-dir -r requirements.txt \
        # Clean up pip cache to make image smaller
        && rm -rf /root/.cache/pip

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