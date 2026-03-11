# FROM python:3.10-slim

# RUN apt-get update && apt-get install -y cron

# RUN touch /var/log/cron.log

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY batchmanager.py .
# COPY crontab /etc/cron.d/mysimbdp-cron

# RUN chmod 0644 /etc/cron.d/mysimbdp-cron
# RUN crontab /etc/cron.d/mysimbdp-cron

# CMD ["cron", "-f"]

FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY batchmanager.py .

CMD ["python", "/app/batchmanager.py"]
