FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

COPY app.requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./app /app
COPY ./database /app/database