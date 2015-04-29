FROM python
RUN conda install -y ipython
RUN conda install -y nose
WORKDIR /code
COPY setup.py /code/
RUN python setup.py build
COPY . /code/
RUN python setup.py install

