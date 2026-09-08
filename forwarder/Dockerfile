FROM python:3.12-slim

WORKDIR /app

# Force stdout/stderr to be unbuffered so print() logs reach Log Analytics in real time.
ENV PYTHONUNBUFFERED=1

COPY forwarder/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY forwarder/forwarder.py .

# No DURATION_SECONDS set here -> forwarder.py runs continuously (see main()).
CMD ["python", "forwarder.py"]
