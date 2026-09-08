FROM python:3.12-slim

WORKDIR /app

COPY forwarder/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY forwarder/forwarder.py .

# No DURATION_SECONDS set here -> forwarder.py runs continuously (see main()).
CMD ["python", "forwarder.py"]
