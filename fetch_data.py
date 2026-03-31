"""
Busca dados do Databricks via SQL Statement API e salva em data.json

Configurar no GitHub Secrets:
  - DATABRICKS_HOST
  - DATABRICKS_TOKEN
  - DATABRICKS_WAREHOUSE_ID
"""

import os
import json
import time
import requests
from datetime import datetime

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

SQL_QUERY = """
SELECT 
    a.prioridade              AS `Prioridade`,
    a.ptr_usuario_nome        AS `Usuário`,
    b.email_corporativo       AS `Email do Usuário`,
    a.ptr_esboco              AS `Esboço`,
    a.ptr_numero              AS `Número do Pedido`,
    a.ptr_segmento            AS `Segmento`,
    a.ptr_data_lancamento     AS `Data de Lançamento`,
    a.ptr_data_esboco         AS `Data do Esboço`,
    a.ptr_valor_total         AS `Valor Total`,
    a.ptr_observacoes         AS `Observações`,
    a.ptr_item_codigo         AS `Código do Item`,
    a.ptr_item_descricao      AS `Descrição do Item`,
    a.data_atualizacao_tabela AS `Data de Atualização`
FROM `gold`.`sap`.`fato_atendimento_pedido_transf_estoque` a
LEFT JOIN (
    SELECT
        nome,
        email_corporativo
    FROM `gold`.`rh`.`fato_funcionario`
    WHERE situacao = 'A - Ativo'
) b
    ON TRIM(UPPER(a.ptr_usuario_nome)) = TRIM(UPPER(b.nome))
WHERE a.ptr_data_esboco >= '2026-01-01'
"""

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


def run_query():
    """Executa a query e aguarda o resultado."""
    resp = requests.post(
        f"{HOST}/api/2.0/sql/statements",
        headers=HEADERS,
        json={
            "warehouse_id": WAREHOUSE_ID,
            "statement": SQL_QUERY,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        },
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()

    statement_id = payload["statement_id"]
    status = payload.get("status", {}).get("state", "")

    for _ in range(20):
        if status in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            break

        time.sleep(3)

        poll = requests.get(
            f"{HOST}/api/2.0/sql/statements/{statement_id}",
            headers=HEADERS,
            timeout=30,
        )
        poll.raise_for_status()
        payload = poll.json()
        status = payload.get("status", {}).get("state", "")

    if status != "SUCCEEDED":
        raise RuntimeError(f"Query falhou com status: {status}")

    return payload


def to_float(value):
    """Converte valor numérico com segurança."""
    if value is None or value == "":
        return 0.0

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def to_str(value):
    """Converte para string sem retornar None."""
    if value is None:
        return ""
    return str(value)


def parse_results(payload):
    """Converte resultado da API em lista de registros no formato do front."""
    manifest = payload.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    rows = payload.get("result", {}).get("data_array", [])

    records = []

    for row in rows:
        r = dict(zip(columns, row))

        records.append({
            "Prioridade": to_str(r.get("Prioridade")),
            "Usuário": to_str(r.get("Usuário")),
            "Esboço": to_str(r.get("Esboço")),
            "Número do Pedido": to_str(r.get("Número do Pedido")),
            "Segmento": to_str(r.get("Segmento")),
            "Data de Lançamento": to_str(r.get("Data de Lançamento")),
            "Data do Esboço": to_str(r.get("Data do Esboço")),
            "Valor Total": to_float(r.get("Valor Total")),
            "Observações": to_str(r.get("Observações")),
            "Código do Item": to_str(r.get("Código do Item")),
            "Descrição do Item": to_str(r.get("Descrição do Item")),
            "Data de Atualização": to_str(r.get("Data de Atualização")),
            "Status": "Pendente",
            "Motivo": "",
            "historico": [],
        })

    return records


def main():
    print("Consultando Databricks...")
    payload = run_query()
    records = parse_results(payload)
    print(f"{len(records)} registros encontrados.")

    output = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "records": records,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("data.json atualizado com sucesso.")


if __name__ == "__main__":
    main()
