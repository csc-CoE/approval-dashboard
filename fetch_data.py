"""
Busca dados do Databricks via SQL Statement API e salva em data.json
Configurar no GitHub Secrets:
  - DATABRICKS_HOST          ex: https://adb-xxxx.azuredatabricks.net
  - DATABRICKS_TOKEN         Personal Access Token do Databricks
  - DATABRICKS_WAREHOUSE_ID  ID do SQL Warehouse
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
    f.prioridade                                        AS prioridade,
    UPPER(f.esboco_usuario_nome)                        AS solicitante,
    r.email_corporativo                                 AS email_solicitante,
    f.ptr_esboco                                        AS n_doesboco,
    f.ptr_segmento                                      AS segmento,
    DATE_FORMAT(f.ptr_data_esboco, 'dd/MM/yyyy')        AS data,
    f.ptr_valor_total                                   AS line_total,
    f.ptr_observacoes                                   AS observacoes,
    f.ptr_item_codigo                                   AS item_code,
    f.ptr_item_descricao                                AS dscription,
    f.ptr_material_status                               AS status_sap,
    DATE_FORMAT(
        FROM_UTC_TIMESTAMP(f.data_atualizacao_tabela, 'America/Sao_Paulo'),
        'dd/MM/yyyy HH:mm'
    )                                                   AS data_atualizacao
FROM gold.sap.fato_atendimento_pedido_transf_estoque f
LEFT JOIN (
    SELECT
        UPPER(nome)        AS nome,
        email_corporativo
    FROM gold.rh.fato_funcionario
    WHERE situacao <> 'D - Demitido'
) r ON UPPER(f.esboco_usuario_nome) = r.nome
WHERE f.ptr_data_esboco >= '2026-01-01'
  AND f.ptr_numero IS NULL
ORDER BY f.ptr_data_esboco DESC
"""

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


def run_query():
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


def parse_results(payload):
    manifest = payload.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    rows = payload.get("result", {}).get("data_array", [])

    records = []
    for i, row in enumerate(rows):
        r = dict(zip(columns, row))
        records.append({
            "id":                i,
            "n_doesboco":        r.get("n_doesboco", ""),
            "prioridade":        r.get("prioridade", ""),
            "data":              r.get("data", ""),
            "solicitante":       r.get("solicitante", ""),
            "email_solicitante": r.get("email_solicitante", ""),
            "segmento":          r.get("segmento", ""),
            "item_code":         r.get("item_code", ""),
            "dscription":        r.get("dscription", ""),
            "line_total":        float(r.get("line_total") or 0),
            "observacoes":       r.get("observacoes", ""),
            "status_sap":        r.get("status_sap", ""),
            "data_atualizacao":  r.get("data_atualizacao", ""),
            # campos gerenciados pelo painel
            "status":            r.get("status_sap", "Pendente"),
            "motivo":            "",
            "historico":         [],
        })
    return records


def main():
    print("Consultando Databricks...")
    payload = run_query()
    records = parse_results(payload)
    print(f"  {len(records)} registros encontrados.")

    output = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "records": records,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("data.json salvo com sucesso.")


if __name__ == "__main__":
    main()
