FROM python:3.11-slim

WORKDIR /ge
# Copy only the project folder contents into /ge
COPY . .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
	&& pip install --no-cache-dir great_expectations \
	&& ln -sf /usr/local/lib/python3.10/site-packages/great_expectations/cli.py /usr/local/bin/great_expectations

CMD ["sleep", "infinity"]
