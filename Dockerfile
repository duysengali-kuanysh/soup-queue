FROM python:3.12-slim

WORKDIR /srv/app

ENV PYTHONPATH=/srv/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]0.0", "--port", "8000"]  