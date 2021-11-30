#Deriving the latest base image
FROM python:latest


COPY ./requirements.txt /requirements.txt

RUN pip install -r requirements.txt

COPY . /data_pipeline_example

WORKDIR data_pipeline_example

COPY run.py ./

CMD [ "python", "./run.py"]