# # 1
# FROM python:3.9

# RUN pip install pandas

# WORKDIR /app
# COPY pipeline.py pipeline.py

# ENTRYPOINT [ "python", "pipeline.py" ]

# 2

FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 fastparquet

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]