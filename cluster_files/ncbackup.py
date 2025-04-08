import zmq
import socket
import re
import threading
import time
import random
import queue
import os

# Configuração da rede
TOKEN_PORT = 6000  # Porta base para comunicação entre nós
CLIENT_PORT = 7000  # Porta base para comunicação com clientes
STORE_PORT = 9000  # Porta base para comunicação com stores

def get_pod_id():
    hostname = socket.gethostname()
    match = re.search(r'cluster-node-(\d+)', hostname)
    return int(match.group(1)) if match else -1

NODE_ID = get_pod_id()

TOTAL_NODES = 5  # Número total de nós na rede
NEXT_NODE_ID = (NODE_ID + 1) % TOTAL_NODES

MY_TOKEN_ADDR = f"tcp://*:{TOKEN_PORT}"
NEXT_NODE = f"tcp://cluster-node-{NEXT_NODE_ID}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"
CLIENT_RECEIVE = f"tcp://*:{CLIENT_PORT}"
CLIENT_SEND = f"tcp://client-node-{NODE_ID}.client-service.default.svc.cluster.local:{CLIENT_PORT}"
# Contexto do ZeroMQ
context = zmq.Context()

received_buffer = queue.Queue()

# Função para formatar saídas
def print_log(msg, tipo="info"):
    cores = {
        "info": "\033[94m",    # Azul
        "sucesso": "\033[92m", # Verde
        "alerta": "\033[93m",  # Amarelo
        "erro": "\033[91m",    # Vermelho,
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
    print(f"{cor}[Nó {NODE_ID}] {emj} {msg}{cores['reset']}", flush=True)

# Receber pedidos dos clientes
def client_receiver_thread():
    socket = context.socket(zmq.PULL)
    socket.bind(CLIENT_RECEIVE)
    while True:
        msg = socket.recv_pyobj()
        threading.Thread(target=save_on_buffer_thread, args=(msg,)).start()

# Receber o token do nó anterior e gerenciar a fila de requisições
def token_receiver_thread():
    time.sleep(1)
    socket = context.socket(zmq.PULL)
    socket.bind(MY_TOKEN_ADDR)
    while True:
        token = socket.recv_pyobj()
        time.sleep(random.uniform(0.2, 0.3))
        if token[NODE_ID][1] != -1:
            is_smallest = True
            for token_msg in token:
                if token_msg[1] != -1 and token_msg[1] < token[NODE_ID][1]:
                    is_smallest = False
                    break
            if is_smallest:
                print_log("🛑 Entrando na zona crítica", "alerta")
                threads = []
                # Adiciona thread para o pedido principal do token
                threads.append(threading.Thread(target=process_and_ack, args=(token[NODE_ID],)))
                # Adiciona threads para pedidos adicionais na fila
                while not received_buffer.empty():
                    request = received_buffer.get()
                    threads.append(threading.Thread(target=process_and_ack, args=(request,)))
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                token[NODE_ID] = ("", -1)
                print_log("✅ Saindo da zona crítica", "sucesso")
        if not received_buffer.empty() and token[NODE_ID][1] == -1:
            token[NODE_ID] = received_buffer.get()
        next_socket = context.socket(zmq.PUSH)
        next_socket.connect(NEXT_NODE)
        next_socket.send_pyobj(token)

# Criador inicial do token (apenas o nó 4)
def token_creator_thread():
    if NODE_ID == 4:
        time.sleep(2)
        socket = context.socket(zmq.PUSH)
        socket.connect(NEXT_NODE)
        print_log("🌀 Criou o token", "info")
        time.sleep(1)
        token = [("", -1)] * TOTAL_NODES
        socket.send_pyobj(token)
        print_log("🚀 Iniciou o token", "info")

# Processa e envia a resposta ao cliente
def process_and_ack(message):
    time.sleep(random.uniform(0.2, 1.0))
    print_log(f"🧠 Processou requisição | 🧍 Cliente: {message[0]} | ⏱️ Timestamp: {message[1]}", "sucesso")

    key = f"{message[1]}"
    valor = f"value_content_{NODE_ID}_{message[1]}"
    conteudo = {"key": key, "valor": valor, "migrate": True}

    # Randomiza lista de stores_ids para realizar tentativas
    MAX_RETRIES = 3
    store_ids = list(range(3))
    random.shuffle(store_ids)
    

    ack = None
    for attempt, store_id in enumerate(store_ids[:MAX_RETRIES]):
        store_addr = F"tcp://store-node-{store_id}.store-service.default.svc.cluster.local:{STORE_PORT}"
        print_log(f"📦 Tentando update no store {store_id} para key: {key} ({attempt + 1} tentativa), Ordem de Stores para tentativa: {store_ids}", "info")
        try:
            store_socket = context.socket(zmq.REQ)
            store_socket.setsockopt(zmq.LINGER, 0)
            store_socket.setsockopt(zmq.RCVTIMEO, 6000)
            store_socket.connect(store_addr)
            store_socket.send_pyobj({"tipo": "ATUALIZACAO", "conteudo": conteudo})
            ack = store_socket.recv_pyobj()
            store_socket.close()
            
            # Printar resumo se conectou com o store
            if ack.get("status") == "ok":
                falhas = ack.get("falhas", [])
                falhas_str = ""
                if falhas:
                    falhas_str = "\n".join([
                        f"\033[91m⚠️ Backup {f['store_id']} indisponível ou timeout: {f.get('reason', 'motivo desconhecido')}\033[0m"
                        for f in falhas
                    ])

                # Apresenta resumo final
                print_log(
                    f"🔗 Conectado com \033[96mCluster Store {store_id}\033[0m como PRIMÁRIO 🟢 | "
                    f"🧍 Cliente: {message[0]} | ⏱️ Timestamp: {message[1]} | "
                    f"🧱 Requisição processada por Nó {NODE_ID}" +
                    (f"\n📉 Resumo da replicação com falhas:\n{falhas_str}" if falhas else ""),
                    "alerta"
                )
                break
            else:
                print_log(f"❌ Store {store_id} respondeu erro: {ack.get('reason')}", "erro")
        except Exception as e:
            print_log(f"❌ Falha ao acessar store {store_id}: Timeout ou indisponível", "erro")

    if ack is None or ack.get("status") != "ok":
        print_log(f"🛑 Nenhum store disponível! Não foi possível aplicar o update para {key}", "erro")

    # Envia resposta ao cliente
    socket = context.socket(zmq.PUSH)
    socket.connect(CLIENT_SEND)
    socket.send_pyobj(message)
    print_log(f"📤 Enviou resposta para o cliente: {message[0]}", "sucesso")
    socket.close()

# Armazena pedido recebido
def save_on_buffer_thread(message):
    if message is not None:
        print_log(f"📬 Recebeu pedido do cliente: {message[0]} | timestamp: {message[1]}", "info")
        received_buffer.put(message)
    else:
        print_log("❗ Erro ao receber pedido", "erro")

# Inicia todas as threads
threads = [
    threading.Thread(target=client_receiver_thread, daemon=True),
    threading.Thread(target=token_receiver_thread, daemon=True),
    threading.Thread(target=token_creator_thread, daemon=True)
]
time.sleep(1)
for t in threads:
    t.start()
for t in threads:
    t.join()