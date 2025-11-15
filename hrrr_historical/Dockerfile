FROM python:3.11-slim

# Set working directory
WORKDIR /hrrr_historical

# Install system dependencies required for weather data processing
RUN apt-get update && apt-get install -y \
    cron \
    nano \
    curl \
    wget \
    ca-certificates \
    libeccodes0 \
    libeccodes-dev \
    libproj-dev \
    libgeos-dev \
    libssl-dev \
    build-essential \
    gfortran \
    libnetcdf-dev \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the application code 
COPY . .

# Set permissions for crontab file and install it
RUN chmod 0644 crontab.txt && crontab crontab.txt

# Add application directory to PATH
ENV PATH="$PATH:/hrrr_historical"

# Set environment variables for GRIB processing
ENV ECCODES_DIR=/usr

# Run cron in the foreground (for Docker)
CMD ["cron", "-f", "-l", "2"]