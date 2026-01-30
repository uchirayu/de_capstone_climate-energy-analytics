FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN apt-get update \
	&& apt-get install -y curl && rm -rf /var/lib/apt/lists/* \
	&& pip install -r requirements.txt

COPY app/ .
RUN chmod +x /app/wait_and_run_simulator.sh || true

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]