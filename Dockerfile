#Use python 3.6.10
FROM python:3.6.10

MAINTAINER Abby Mu

#Make port 9066 available
EXPOSE 9066

ADD requirements.txt

#Install required environment
RUN pip install -r requirements.txtflask

# Run yolodetect.py
CMD ["python","yolodetect.py"]
