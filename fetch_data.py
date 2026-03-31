"""
Busca dados do Databricks via SQL Statement API e salva em data.json
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta

HOST = os.environ["DATABRICKS_HOST"].strip().rstrip("/")
if not HOST.startswith("http://") and not HOST.startswith("https://"):
    HOST = "https://" + HOST

TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"].strip().split("/")[-1]

SQL_QUERY = """
SELECT 
    prioridade AS `Prioridade`,
    ptr_usuario_nome AS `Responsavel Esboço`,
    ptr_esboco AS `Numero Esboço`,
    ptr_segmento AS `Segmento`,
    ptr_data_esboco AS `Data do Esboço`,
    ptr_valor_total AS `Valor Total`,
    ptr_observacoes AS `Observações`,
    ptr_item_codigo AS `Código do Item`,
    ptr_item_descricao AS `Descrição do Item`,
    ptr_material_status AS `Status Esboço`,
    date_format(
        from_utc_timestamp(data_atualizacao_tabela, 'America/Sao_Paulo'),
        'dd/MM/yyyy HH:mm'
    ) AS `Data de Atualização`
FROM `gold`.`sap`.`fato_atendimento_pedido_transf_estoque`
WHERE ptr_data_esboco >= '2026-01-01'
  AND ptr_numero IS NULL
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

    if not resp.ok:
        print("Erro ao submeter statement:")
        print("Status code:", resp.status_code)
        print("Response text:", resp.text)
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

        if not poll.ok:
            print("Erro no polling do statement:")
            print("Status code:", poll.status_code)
            print("Response text:", poll.text)
            poll.raise_for_status()

        payload = poll.json()
        status = payload.get("status", {}).get("state", "")

    if status != "SUCCEEDED":
        print("Payload final:", json.dumps(payload, ensure_ascii=False, indent=2))
        raise RuntimeError(f"Query falhou com status: {status}")

    return payload


def to_float(value):
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def to_str(value):
    if value is None:
        return ""
    return str(value)


def map_status(status_esboco, numero_ptr):
    numero_ptr = to_str(numero_ptr).strip()
    status_esboco = to_str(status_esboco).strip().lower()

    if numero_ptr:
        return "Esboço Fechado"

    # como sua query já filtra ptr_numero is null, quase tudo aqui será pendente
    return "Esboço Pendente"


def map_situacao_tratamento(status_esboco):
    s = to_str(status_esboco).strip().lower()

    if "insuficiente" in s or "sem saldo" in s:
        return "Sem saldo em estoque"
    if "erro" in s:
        return "Erro no SAP"
    if "cancel" in s:
        return "Cancelado"
    if "atendido" in s:
        return "Atendido"
    if "parcial" in s:
        return "Em tratativa com Estoque"

    return "Em análise pelo time"


def parse_results(payload):
    manifest = payload.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    rows = payload.get("result", {}).get("data_array", [])

    records = []

    for row in rows:
        r = dict(zip(columns, row))

        status_oficial = to_str(r.get("Status Esboço")).strip()
        if not status_oficial:
            status_oficial = "Esboço Pendente"

        situacao_tratamento = "Em análise pelo time" if status_oficial == "Esboço Pendente" else ""

        records.append({
            "Prioridade": to_str(r.get("Prioridade")),
            "Responsavel Esboço": to_str(r.get("Responsavel Esboço")),
            "Numero Esboço": to_str(r.get("Numero Esboço")),
            "Segmento": to_str(r.get("Segmento")),
            "Data do Esboço": to_str(r.get("Data do Esboço")),
            "Valor Total": to_float(r.get("Valor Total")),
            "Observações": to_str(r.get("Observações")),
            "Código do Item": to_str(r.get("Código do Item")),
            "Descrição do Item": to_str(r.get("Descrição do Item")),
            "Status": status_oficial,
            "Data de Atualização": to_str(r.get("Data de Atualização")),
            "Situação do Tratamento": situacao_tratamento,
            "historico": [],
        })

    return records

def main():
    print("Consultando Databricks...")
    payload = run_query()
    records = parse_results(payload)
    print(f"{len(records)} registros encontrados.")

    agora_brasil = datetime.utcnow() - timedelta(hours=3)

    output = {
        "updated_at": agora_brasil.strftime("%d/%m/%Y %H:%M BRT"),
        "records": records,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("data.json atualizado com sucesso.")


if __name__ == "__main__":
    main()
