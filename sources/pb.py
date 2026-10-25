"""Acesso ao código gerado a partir de proto/sources.proto."""

try:
    from proto import sources_pb2 as pb
    from proto import sources_pb2_grpc as pb_grpc
except ImportError as exc:  # pragma: no cover - só acontece sem gerar o código
    raise ImportError(
        "Código protobuf não encontrado. Rode na raiz do repositório: python scripts/gen_proto.py"
    ) from exc

__all__ = ["pb", "pb_grpc"]
