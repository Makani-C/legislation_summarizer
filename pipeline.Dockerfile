FROM python:3.10

COPY pipeline.requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./pipeline /pipeline
COPY ./database /pipeline/database

CMD ["python", "pipeline/pipeline.py"]