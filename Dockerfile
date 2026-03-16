# Use lightweight Python 3.11 image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Upgrade pip and install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Expose port 8000 for FastAPI
EXPOSE 8000

# Set default environment variables (can be overridden at runtime)
ENV DERIV_API_TOKEN="YOUR_API_TOKEN"
ENV DERIV_APP_ID="1089"
ENV TIMEFRAME="60"
ENV CANDLE_COUNT="80"
ENV MAX_HISTORY="150"
ENV SCAN_INTERVAL="1.0"

# Run FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
