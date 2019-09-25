FROM debian:8.5

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apt-get update --fix-missing && apt-get install -y wget bzip2 ca-certificates \
    libglib2.0-0 libxext6 libsm6 libxrender1 \
    git mercurial subversion

# The --no-check-certificate is due to internal SSL issues 
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    wget --no-check-certificate --quiet https://repo.continuum.io/miniconda/Miniconda3-4.3.21-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

RUN apt-get install -y curl grep sed dpkg && \
    TINI_VERSION=`curl -k https://github.com/krallin/tini/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'` && \
    curl -k -L "https://github.com/krallin/tini/releases/download/v${TINI_VERSION}/tini_${TINI_VERSION}.deb" > tini.deb && \
    dpkg -i tini.deb && \
    rm tini.deb && \
    apt-get clean

ENV PATH /opt/conda/bin:$PATH

ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "/bin/bash" ]

# To get a gcc compiler in for building C libs
RUN apt-get -y install build-essential

# Get PGAdmin4 installed w/ dependencies
RUN pip install https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v1.6/pip/pgadmin4-1.6-py2.py3-none-any.whl --trusted-host=ftp.postgresql.org --trusted-host=pypi.python.org

# PGAdmin port
EXPOSE 5050

# Set the server parameter to be false
RUN sed -i 's/SERVER_MODE = True/SERVER_MODE = False/g' /opt/conda/lib/python3.6/site-packages/pgadmin4/config.py
RUN sed -i "s/DEFAULT_SERVER = '127.0.0.1'/DEFAULT_SERVER = '0.0.0.0'/g" /opt/conda/lib/python3.6/site-packages/pgadmin4/config.py 

RUN python /opt/conda/lib/python3.6/site-packages/pgadmin4/config.py

CMD ["python", "/opt/conda/lib/python3.6/site-packages/pgadmin4/pgAdmin4.py"]