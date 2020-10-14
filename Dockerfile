FROM python:3.7-alpine

RUN apk update

# CREATE A WORKING DIR INSIDE CONTAINER
WORKDIR /usr/src/app

COPY requirements.txt .

# install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# COPY entire project from local m/c to working dir in the container
COPY . .

RUN python --version

# Run the app
CMD ["python3", "./src/app.py"]
