# Dockerfile

# The first instruction is what image we want to base our container on
# We Use an official Python runtime as a parent image
FROM python:3.9

COPY . /usr/src/

WORKDIR /usr/src

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install -r venv_reqs.txt

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]