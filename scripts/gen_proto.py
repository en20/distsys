"""Gera o código Python a partir de todos os arquivos proto/*.proto.

Uso (na raiz do repositório):  python scripts/gen_proto.py

Os arquivos gerados ficam ao lado dos .proto (proto/*_pb2.py, proto/*_pb2_grpc.py)
e são importados como `from proto import sources_pb2`.
"""

import sys
from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    protos = sorted(str(p.relative_to(ROOT)) for p in (ROOT / "proto").glob("*.proto"))
    if not protos:
        print("nenhum .proto encontrado em proto/", file=sys.stderr)
        return 1
    include = str(Path(protoc.__file__).parent / "_proto")  # tipos padrão (google/protobuf/*)
    args = ["protoc", f"-I{ROOT}", f"-I{include}",
            f"--python_out={ROOT}", f"--pyi_out={ROOT}", f"--grpc_python_out={ROOT}", *protos]
    code = protoc.main(args)
    if code == 0:
        print("gerado:", ", ".join(protos))
    return code


if __name__ == "__main__":
    sys.exit(main())
