FROM python:3.12.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock contracts.lock.json ./
COPY .artifacts/local_llm_contract-1.0.0-py3-none-any.whl /tmp/contracts/
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.lock && python -c "import hashlib,json,pathlib; p=pathlib.Path('/tmp/contracts/local_llm_contract-1.0.0-py3-none-any.whl'); assert hashlib.sha256(p.read_bytes()).hexdigest()==json.load(open('contracts.lock.json'))['sha256']" && python -m pip install --no-deps /tmp/contracts/*.whl
COPY pyproject.toml ./
COPY app ./app
RUN python -m pip install --no-deps . && useradd --uid 10001 --create-home review
USER 10001
EXPOSE 8080
STOPSIGNAL SIGTERM
CMD ["review-api"]
