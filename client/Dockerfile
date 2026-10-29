# Imagem Node do Cliente (dashboard web e CLI). Contexto de build: a raiz do repositório,
# porque o cliente carrega proto/client.proto em tempo de execução.
FROM node:22-slim

WORKDIR /app/client

COPY client/package.json client/package-lock.json ./
RUN npm ci --omit=dev

COPY proto /app/proto
COPY client ./

ENV DISTSYS_GATEWAY_HOST=gateway

EXPOSE 8080
CMD ["node", "web/server.js", "--bind", "0.0.0.0"]
