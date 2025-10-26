FROM python:3.10-alpine

WORKDIR /app

RUN apk add --no-cache gcc musl-dev libffi-dev openssl-dev openssh-client

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app /app