FROM mjhea0/spark:2.4.1

RUN apt-get update

RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pip
RUN pip3 install numpy pandas sklearn

# COPY /. //app
# COPY model.py //app

# ENTRYPOINT ["bin/spark-submit", "--master", "spark://master:7077", "//app/model.py"]