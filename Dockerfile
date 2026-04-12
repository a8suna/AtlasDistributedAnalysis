FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    uproot \
    vector \
    awkward \
    numpy \
    matplotlib \
    pika \
    requests \
    aiohttp \
    fsspec \
    atlasopenmagic

COPY src/ .

CMD ["python", "worker.py"]