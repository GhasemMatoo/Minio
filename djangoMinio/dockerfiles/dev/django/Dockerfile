FROM python:3.9-slim-buster

LABEL maintainer="bmatoogsm90@gmail.com"

ENV PYTHONUNBUFFERED=1

WORKDIR /djangoMinio/

COPY requirements.txt .

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

COPY . /djangoMinio/


# collecting static files
CMD python3 manage.py collectstatic --no-input