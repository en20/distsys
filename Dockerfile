# Imagem Python do Gateway e das fontes (cada uma roda num container próprio, ver docker-compose.yml).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código protobuf gerado no build (equivale a ./scripts/gen_proto.sh).
COPY proto proto
RUN mkdir -p pb && python -m grpc_tools.protoc -Ipb=proto --python_out=. --pyi_out=. --grpc_python_out=. proto/*.proto

COPY common common
COPY gateway gateway
COPY sources sources

CMD ["python", "-m", "gateway.main"]
