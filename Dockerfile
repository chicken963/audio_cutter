FROM python:3.9-slim

RUN apt-get update && apt-get install -y ffmpeg
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY kafka_utils.py s3_utils.py venv config.yaml main.py requirements.txt /app/

RUN python -m venv venv
RUN /bin/bash -c "source venv/bin/activate"
RUN pip install -r requirements.txt
EXPOSE 80

CMD ["python", "main.py"]