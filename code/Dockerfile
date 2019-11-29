FROM jupyter/pyspark-notebook:latest

COPY /script/ //app

ENTRYPOINT ["//usr/local/spark/bin/spark-submit", "--master", "spark://master:7077", "//app/pred.py"]
CMD ["day,DI,2,neige"]