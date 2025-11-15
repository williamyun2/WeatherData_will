FROM python:3.11-slim

# Set working directory to match your actual folder name
WORKDIR /efield

# Install cron and nano
RUN apt-get update && apt-get install -y cron nano && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Set permissions for crontab file and install it (fixed filename)
RUN chmod 0644 crontab.txt && crontab crontab.txt

# Add application directory to PATH (updated path)
ENV PATH="$PATH:/efield"

# Run cron in the foreground (for Docker)
CMD ["cron", "-f", "-l", "2"]