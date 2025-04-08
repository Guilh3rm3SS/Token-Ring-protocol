import zmq
import socket
import re
import threading
import time
import random
import queue

# Configuração da rede
TOKEN_PORT = 6000
CLIENT_PORT = 7000
HEARTBEAT_PORT = 8000

def get_pod_id():
    hostname = socket.gethostname()
    match = re.search(r'cluster-node-(\d+)', hostname)
    return int(match.group(1)) if match else -1

NODE_ID = get_pod_id()
TOTAL_NODES = 5

MY_TOKEN_ADDR = f"tcp://*:{TOKEN_PORT}"
NEXT_NODE_ID = (NODE_ID + 1) % TOTAL_NODES
NEXT_NODE = f"tcp://cluster-node-{NEXT_NODE_ID}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"

CLIENT_RECEIVE = f"tcp://*:{CLIENT_PORT}"
CLIENT_SEND = f"tcp://client-node-{NODE_ID}.client-service.default.svc.cluster.local:{CLIENT_PORT}"

MY_HEARTBEAT_ADDR = f"tcp://*:{HEARTBEAT_PORT}"
HEARTBEAT_ADDRS = [
    f"tcp://cluster-node-{i}.cluster-node-service.default.svc.cluster.local:{HEARTBEAT_PORT}"
    for i in range(TOTAL_NODES)
]

context = zmq.Context()
received_buffer = queue.Queue()
processed_buffer = queue.Queue()
last_token = None

replaced_clients = set()

# ---------- HEARTBEAT ----------

def heartbeat_responder_thread():
    socket = context.socket(zmq.REP)
    socket.bind(MY_HEARTBEAT_ADDR)

    while True:
        try:
            msg = socket.recv_string()
            if msg == "ping":
                socket.send_string("pong")
        except Exception as e:
            print(f"[Nó {NODE_ID}] Erro no heartbeat responder: {e}", flush=True)

def is_node_alive(node_id, timeout=2):
    addr = HEARTBEAT_ADDRS[node_id]
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.RCVTIMEO = timeout * 1000
    try:
        socket.connect(addr)
        socket.send_string("ping")
        reply = socket.recv_string()
        return reply == "pong"
    except:
        return False
    finally:
        socket.close()

# ---------- CLIENTES ----------

def client_receiver_thread():
    socket = context.socket(zmq.PULL)
    socket.bind(CLIENT_RECEIVE)

    while True:
        msg = socket.recv_pyobj()
        threading.Thread(target=save_on_buffer_thread, args=(msg,)).start()

def client_sender_thread():
    socket = context.socket(zmq.PUSH)
    socket.connect(CLIENT_SEND)

    while True:
        if not processed_buffer.empty():
            msg = processed_buffer.get()
            socket.send_pyobj(msg)
            print(f"[Nó {NODE_ID}] Enviou resposta para o cliente: {msg[0]} {msg[2]}", flush=True)

# ---------- TOKEN ----------

def monitor_token_loss():
    global last_token
    TOKEN_TIMEOUT = 30  # tempo de espera antes de considerar o token como perdido (em segundos)

    while True:
        time.sleep(TOKEN_TIMEOUT)

        # Verifica se o token está sumido por tempo demais
        if NODE_ID == 0:
            if last_token is None or time.time() - last_token['timestamp'] > TOKEN_TIMEOUT:
                print(f"[Nó 0] Token perdido! Recriando novo token...", flush=True)

                # Novo token "zerado"
                new_token = {
                    "owner": NODE_ID,
                    "buffer": [],
                    "timestamp": time.time()
                }

                # Encontra o próximo nó ativo no anel
                next_id = (NODE_ID + 1) % TOTAL_NODES
                while not is_node_alive(next_id) and next_id != NODE_ID:
                    next_id = (next_id + 1) % TOTAL_NODES

                # Caso não haja outro nó ativo, não envia
                if next_id == NODE_ID:
                    print("[Nó 0] Nenhum outro nó ativo. Aguardando...", flush=True)
                    continue

                # Endereço do próximo nó
                addr = f"tcp://cluster-node-{next_id}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"
                
                try:
                    s = context.socket(zmq.PUSH)
                    s.connect(addr)
                    s.send_pyobj(new_token)
                    last_token = new_token  # Atualiza o controle interno
                    print(f"[Nó 0] Novo token enviado para nó {next_id}", flush=True)
                    s.close()
                except Exception as e:
                    print(f"[Nó 0] Erro ao tentar enviar token recriado: {e}", flush=True)



def token_receiver_thread():
    global last_token
    count = 0
    socket = context.socket(zmq.PULL)
    socket.bind(MY_TOKEN_ADDR)

    while True:
        try:
            token = socket.recv_pyobj()
            last_token = token
            last_token['timestamp'] = time.time()
            print(f"[Nó {NODE_ID}] Recebeu token", flush=True)
            count += 1
            if NODE_ID == 3 and count > 10:
                exit(1)

            if token[NODE_ID][1] != -1:
                is_smallest = all(token_msg[1] == -1 or token_msg[1] >= token[NODE_ID][1] for token_msg in token)

                if is_smallest:
                    print(f"[Nó {NODE_ID}] Entrando na zona crítica", flush=True)

                    threads = [threading.Thread(target=lambda: processed_buffer.put(request_processing_thread(token[NODE_ID])))]

                    while not received_buffer.empty():
                        request = received_buffer.get()
                        threads.append(threading.Thread(target=lambda req=request: processed_buffer.put(request_processing_thread(req))))

                    for t in threads:
                        t.start()
                    for t in threads:
                        t.join()

                    token[NODE_ID] = ("", -1)
                    print(f"[Nó {NODE_ID}] Saindo da zona crítica", flush=True)

            if not received_buffer.empty() and token[NODE_ID][1] == -1:
                token[NODE_ID] = received_buffer.get()

            next_id = (NODE_ID + 1) % TOTAL_NODES
            found = False

            for _ in range(TOTAL_NODES - 1):
                if is_node_alive(next_id):
                    try:
                        next_addr = f"tcp://cluster-node-{next_id}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"
                        next_socket = context.socket(zmq.PUSH)
                        next_socket.connect(next_addr)
                        next_socket.send_pyobj(token)
                        print(f"[Nó {NODE_ID}] Enviou token para o nó {next_id}", flush=True)
                        found = True
                        break
                    except Exception as e:
                        print(f"[Nó {NODE_ID}] Erro ao enviar token para o nó {next_id}: {e}", flush=True)
                next_id = (next_id + 1) % TOTAL_NODES

            if not found:
                print(f"[Nó {NODE_ID}] Nenhum nó ativo encontrado para envio do token", flush=True)

        except Exception as e:
            print(f"[Nó {NODE_ID}] Erro ao receber token: {e}", flush=True)

def token_creator_thread():
    if NODE_ID == 4:
        time.sleep(2)
        socket = context.socket(zmq.PUSH)
        socket.connect(NEXT_NODE)
        print(f"[Nó {NODE_ID}] Criou o token", flush=True)
        token = [("", -1)] * TOTAL_NODES
        socket.send_pyobj(token)
        print(f"[Nó {NODE_ID}] Iniciou o token", flush=True)

# ---------- PROCESSAMENTO ----------

def request_processing_thread(message):
    time.sleep(random.uniform(0.2, 1.0))
    print(f"[Nó {NODE_ID}] Requisição processada {message[1]}", flush=True)
    return message

def save_on_buffer_thread(message):
    if message is not None:
        print(f"[Nó {NODE_ID}] Recebeu pedido do cliente: {message[0]} | timestamp: {message[1]}", flush=True)
        received_buffer.put(message)
    else:
        print(f"[Nó {NODE_ID}] Erro ao receber pedido", flush=True)

# ---------- DETECÇÃO DE FALHAS ----------

def detect_node_failures():
    global last_token

    reassumed_ports = {}  # chave: nó, valor: (socket, stop_event, thread)

    while True:
        time.sleep(15)
        for i in range(TOTAL_NODES):
            if i == NODE_ID:
                continue

            alive = is_node_alive(i)

            if not alive:
                print(f"[Nó {NODE_ID}] Detectou falha no nó {i}", flush=True)

                if i not in reassumed_ports:
                    try:
                        # Atribui a responsabilidade da porta do nó i ao primeiro nó ativo depois dele
                        reassumer = (i + 1) % TOTAL_NODES
                        while reassumer != NODE_ID and not is_node_alive(reassumer):
                            reassumer = (reassumer + 1) % TOTAL_NODES
                        if reassumer != NODE_ID:
                            continue  # outro nó vai assumir

                        proxy_socket = context.socket(zmq.PULL)
                        proxy_addr = f"tcp://*:{CLIENT_PORT + i + 1}"
                        proxy_socket.bind(proxy_addr)

                        stop_event = threading.Event()

                        def client_proxy_thread(sock, origin_id, stop_evt):
                            poller = zmq.Poller()
                            poller.register(sock, zmq.POLLIN)
                            while not stop_evt.is_set():
                                socks = dict(poller.poll(timeout=1000))
                                if sock in socks and socks[sock] == zmq.POLLIN:
                                    try:
                                        msg = sock.recv_pyobj()
                                        print(f"[Nó {NODE_ID}] Requisição de cliente do nó {origin_id} reassumida", flush=True)
                                        threading.Thread(target=save_on_buffer_thread, args=(msg,), daemon=True).start()
                                    except zmq.ZMQError:
                                        break

                        t = threading.Thread(target=client_proxy_thread, args=(proxy_socket, i, stop_event), daemon=True)
                        t.start()

                        reassumed_ports[i] = (proxy_socket, stop_event, t)
                        print(f"[Nó {NODE_ID}] Reassumiu porta de cliente do nó {i}", flush=True)

                    except Exception as e:
                        print(f"[Nó {NODE_ID}] Não conseguiu reassumir porta do nó {i}: {e}", flush=True)

                # reenviar token
                if last_token:
                    new_dest = (i + 1) % TOTAL_NODES
                    while not is_node_alive(new_dest) and new_dest != NODE_ID:
                        new_dest = (new_dest + 1) % TOTAL_NODES

                    if new_dest != NODE_ID:
                        addr = f"tcp://cluster-node-{new_dest}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"
                        try:
                            token_socket = context.socket(zmq.PUSH)
                            token_socket.connect(addr)
                            token_socket.send_pyobj(last_token)
                            print(f"[Nó {NODE_ID}] Reenviou cópia do token para o nó {new_dest}", flush=True)
                        except Exception as e:
                            print(f"[Nó {NODE_ID}] Erro ao reenviar token: {e}", flush=True)

            else:
                if i in reassumed_ports:
                    print(f"[Nó {NODE_ID}] Nó {i} voltou, liberando porta reassumida", flush=True)
                    try:
                        sock, stop_event, thread = reassumed_ports[i]
                        stop_event.set()
                        thread.join(timeout=2)
                        sock.close(linger=0)
                        del reassumed_ports[i]
                    except Exception as e:
                        print(f"[Nó {NODE_ID}] Erro ao liberar porta do nó {i}: {e}", flush=True)



# ---------- EXECUÇÃO ----------

threads = [
    threading.Thread(target=client_receiver_thread, daemon=True),
    threading.Thread(target=client_sender_thread, daemon=True),
    threading.Thread(target=token_receiver_thread, daemon=True),
    threading.Thread(target=token_creator_thread, daemon=True),
    threading.Thread(target=heartbeat_responder_thread, daemon=True),
    threading.Thread(target=detect_node_failures, daemon=True),
    threading.Thread(target=monitor_token_loss, daemon=True),
]

time.sleep(10)
for t in threads:
    t.start()

for t in threads:
    t.join()
