FROM python:3
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
#RUN mkdir /hackernews
#WORKDIR /hackernews
COPY requirements.txt /
RUN pip install -r requirements.txt
COPY . /
CMD python kafka-consumer-mongo.py
