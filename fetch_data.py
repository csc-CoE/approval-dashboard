"""
Busca dados do Databricks via SQL Statement API e salva em data.json
Configurar no GitHub Secrets:
  - DATABRICKS_HOST        ex: https://adb-xxxx.azuredatabricks.net
  - DATABRICKS_TOKEN       Personal Access Token do Databricks
  - DATABRICKS_WAREHOUSE_ID  ID do SQL Warehouse
"""

import os
import json
import requests
from datetime import datetime

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

# ── Altere a query conforme sua tabela no Databricks ──────────────────────────
SQL_QUERY = """
SELECT
    n_aprovacao,
    n_doesboco,
    DATE_FORMAT(datadacriacao, 'dd/MM/yyyy') AS data,
    solicitante,
    aprovadoratual                            AS aprovador,
    filial,
    dscription,
    SUM(line_total)                           AS line_total,
    status_cabecalho_ow                       AS status
FROM sua_tabela_de_aprovacoes
GROUP BY 1,2,3,4,5,6,7,9
ORDER BY datadacriacao DESC
LIMIT 1000
"""
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}


def run_query():
    """Executa a query e aguarda o resultado."""
    # 1. Submete a query
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

    # 2. Polling se ainda não terminou
    import time
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
    """Converte resultado da API para lista de dicts."""
    manifest = payload.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    rows = payload.get("result", {}).get("data_array", [])

    records = []
    for row in rows:
        r = dict(zip(columns, row))
        records.append({
            "n_aprovacao":  r.get("n_aprovacao", ""),
            "n_doesboco":   r.get("n_doesboco", ""),
            "data":         r.get("data", ""),
            "solicitante":  r.get("solicitante", ""),
            "aprovador":    r.get("aprovador", ""),
            "filial":       r.get("filial", ""),
            "dscription":   r.get("dscription", ""),
            "line_total":   float(r.get("line_total") or 0),
            "status":       r.get("status", "Pendente"),
            "motivo":       "",
            "historico":    [],
        })
    return records


def main():
    print("Consultando Databricks...")
    payload = run_query()
    records = parse_results(payload)
    print(f"  {len(records)} registros encontrados.")

    output = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "records": records,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("data.json atualizado com sucesso.")


if __name__ == "__main__":
    main()
