FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    wget \
    bash

# install and init conda:
RUN mkdir -p ~/miniconda3 && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh && \
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3 && \
    rm ~/miniconda3/miniconda.sh
RUN source ~/miniconda3/bin/activate
RUN conda init --all

# create venv:
RUN conda create -n my_venv python=3.12 -y
RUN conda activate my_venv

# copy sources:
COPY . /app/chunking/*

# intall requirements
WORKDIR /app/chunking/
RUN pip install -r requirements.txt
# set HF_HOME:
ENV HF_HOME=/app/HF_HOME
# run:
CMD ["python", "smart_chunking.py"]