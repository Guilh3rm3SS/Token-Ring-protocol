import zmq
import threading
import time
import os

# Configuração do STORE
STORE_PORT = 8000
STORE_ID = int(os.environ.get("STORE_ID", -1))
MY_STORE_ADDR = f"tcp://*:{STORE_PORT}"

# Armazenamento local e controle de primário
data_store = {}
update_log = []
primary_map = {}

# Contexto ZMQ
context = zmq.Context()


def print_log(msg, tipo="info"):
    cores = {
        "info": "\033[94m",    # Azul
        "sucesso": "\033[92m", # Verde
        "alerta": "\033[93m",  # Amarelo
        "erro": "\033[91m",    # Vermelho
        "reset": "\033[0m"     # Reset
    }
    emoji = {
        "info": "ℹ️",
        "sucesso": "✅",
        "alerta": "⚠️",
        "erro": "❌"
    }
    cor = cores.get(tipo, "")
    emj = emoji.get(tipo, "")
    hora = time.strftime("%H:%M:%S")
    millis = int((time.time() % 1) * 1000)
    print(f"{cor}[{hora},{millis:03d}] {emj} {msg}{cores['reset']}", flush=True)

# === Servidor principal do Cluster Store ===
def cluster_store_server():
    socket_server = context.socket(zmq.REP)
    socket_server.bind(MY_STORE_ADDR)
    print_log(f"🚀 Servidor iniciado em {MY_STORE_ADDR}", "info")

    while True:
        try:
            message = socket_server.recv_pyobj()
            if message:
                result = process_message(message)
                socket_server.send_pyobj(result)
            else:
                socket_server.send_pyobj({"status": "erro", "reason": "mensagem vazia"})
        except Exception as e:
            print_log(f"💥 Erro ao processar mensagem: {e}", "erro")
            socket_server.send_pyobj({"status": "erro", "reason": str(e)})

# === Processamento de mensagens recebidas ===
def process_message(message):
    tipo = message.get("tipo")
    if tipo == "ATUALIZACAO":
        return aplicar_atualizacao(message.get("conteudo"))
    elif tipo == "PING":
        responder_ping()
        return {"status": "ok", "store_id": STORE_ID}
    else:
        print_log(f"⚠️ Tipo de mensagem desconhecido: {tipo}", "alerta")
        return {"status": "erro", "reason": "Tipo de mensagem desconhecido"}

# === Aplicação de atualizações ===
def aplicar_atualizacao(conteudo):
    if not isinstance(conteudo, dict) or "key" not in conteudo or "valor" not in conteudo:
        msg = f"Conteúdo inválido para atualização: {conteudo}"
        print_log(f"❌ {msg}", "erro")
        return {"status": "erro", "reason": msg}

    key = conteudo["key"]
    valor = conteudo["valor"]
    migrate_flag = conteudo.get("migrate", False)
    primary_from_msg = conteudo.get("primary", None)

    if migrate_flag:
        if key in primary_map and primary_map[key] != STORE_ID:
            msg = f"Não sou o primário para {key} (primário atual: {primary_map[key]})"
            print_log(f"⚠️ {msg}", "alerta")
            return {"status": "erro", "reason": msg}
        primary_map[key] = STORE_ID
        print_log(f"ℹ️ Estabelecendo primário para {key} como {STORE_ID}", "info")
    else:
        if primary_from_msg is None:
            msg = f"Backup recebeu update sem indicação de primário!"
            print_log(f"❌ {msg}", "erro")
            return {"status": "erro", "reason": msg}
        primary_map[key] = primary_from_msg

    # Atualiza armazenamento
    data_store[key] = valor
    update_log.append({"time_update": time.time(), "conteudo": conteudo})
    print_log(
        f"✅ Atualização aplicada! 🔑 {key} | 📦 {valor} | 🕒 {time.strftime('%H:%M:%S')} | "
        f"{'⭐ PRIMÁRIO' if migrate_flag else '🔁 Backup'}", "sucesso"
    )

    # Se for primário, propaga para backups
    if migrate_flag:
        rep_msg = {"key": key, "valor": valor, "migrate": False, "primary": STORE_ID}
        resultados_replica = propagar_para_backups_sync(rep_msg)
        falhas = [r for r in resultados_replica if r["status"] != "ok"]

        if falhas:
            return {
                "status": "ok",  # pois aplicou localmente
                "store_id": STORE_ID,
                "replicacao": "parcial" if len(falhas) < 2 else "falha",
                "falhas": falhas
            }
        else:
            return {"status": "ok", "store_id": STORE_ID, "replicacao": "total"}
    else:
        return {"status": "ok", "store_id": STORE_ID}


# === Propagação da atualização para os backups ===
def propagar_para_backups_sync(conteudo):
    resultados = []
    for i in range(3):
        if i == STORE_ID:
            continue  # não replica para si mesmo

        addr = f"tcp://cluster-store-{i}:{STORE_PORT}"
        try:
            socket_backup = context.socket(zmq.REQ)
            socket_backup.setsockopt(zmq.LINGER, 0)
            socket_backup.setsockopt(zmq.RCVTIMEO, 1000)  # timeout de recepção
            socket_backup.connect(addr)
            socket_backup.send_pyobj({"tipo": "ATUALIZACAO", "conteudo": conteudo})
            ack = socket_backup.recv_pyobj()
            if ack.get("status") == "ok":
                print_log(f"🔁 Replicado com sucesso para backup {i}", "sucesso")
                resultados.append({"store_id": i, "status": "ok"})
            else:
                print_log(f"❌ Erro ao replicar para backup {i}: {ack}", "erro")
                resultados.append({"store_id": i, "status": "erro", "reason": ack.get("reason")})
        except Exception as e:
            print_log(f"⚠️ Falha ao replicar Backup: Backup {i} indisponível ou timeout: {e}", "alerta")
            resultados.append({"store_id": i, "status": "erro", "reason": str(e)})
        finally:
            socket_backup.close()
    return resultados


def responder_ping():
    print_log("🟢 Recebeu PING e respondeu.", "info")


if __name__ == "__main__":
    server_thread = threading.Thread(target=cluster_store_server, daemon=True)
    server_thread.start()
    while True:
        time.sleep(0.1)