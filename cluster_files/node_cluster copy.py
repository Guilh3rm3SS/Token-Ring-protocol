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
HEARTBEAT_TOKEN_PORT = 9000
HEARTBEAT_INTERVAL = 1
HEARTBEAT_TIMEOUT = 10

def get_pod_id():
    hostname = socket.gethostname()
    match = re.search(r'cluster-node-(\d+)', hostname)
    return int(match.group(1)) if match else -1

NODE_ID = get_pod_id()
TOTAL_NODES = 5

# Endereços
MY_TOKEN_ADDR = f"tcp://*:{TOKEN_PORT}"  # Usar wildcard para binding
NEXT_TOKEN_ADDR = f"tcp://cluster-node-{(NODE_ID + 1) % TOTAL_NODES}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"
CLIENT_RECEIVE = f"tcp://*:{CLIENT_PORT}"
CLIENT_SEND = f"tcp://client-node-{NODE_ID}.client-service.default.svc.cluster.local:{CLIENT_PORT}"
MY_HEARTBEAT_ADDR = f"tcp://*:{HEARTBEAT_PORT}"
MY_HEARTBEAT_TOKEN_ADDR = f"tcp://*:{HEARTBEAT_TOKEN_PORT}"
NEXT_HEARTBEAT_ADDR = f"tcp://cluster-node-{(NODE_ID + 1) % TOTAL_NODES}.cluster-node-service.default.svc.cluster.local:{HEARTBEAT_PORT}"

# Contexto ZeroMQ
context = zmq.Context()

# Estruturas de dados
received_buffer = queue.Queue()
processed_buffer = queue.Queue()
token_copy_lock = threading.Lock()
active_nodes_lock = threading.Lock()
token_copy = None
active_nodes = [(True, -1)] * TOTAL_NODES

def detect_active_nodes():
    global active_nodes
    socket = context.socket(zmq.REP)
    socket.bind(MY_HEARTBEAT_TOKEN_ADDR)
    socket.setsockopt(zmq.RCVTIMEO, 2000)

    # Aguarda inicialização com mais tempo para sincronização
    print(f"[Nó {NODE_ID}] Aguardando inicialização para detecção de nós...", flush=True)
    time.sleep(20)  # Aumentado para garantir que todos os nós estejam prontos

    while True:
        try:
            token = socket.recv_pyobj(flags=zmq.NOBLOCK)
            with active_nodes_lock:
                active_nodes = token.copy()
            socket.send_pyobj({"status": "ok"})  # Responde ao REQ
            print(f"[Nó {NODE_ID}] Recebeu token de verificação: {active_nodes}", flush=True)
        
        except zmq.Again:
            # Apenas o nó 0 recria o token inicialmente ou após um longo timeout
            if NODE_ID == 0 and not token_exists():
                print(f"[Nó {NODE_ID}] Timeout na recepção do token de verificação. Recriando...", flush=True)
                recreate_verification_token()
            else:
                print(f"[Nó {NODE_ID}] Timeout na recepção do token de verificação. Aguardando...", flush=True)
            time.sleep(1)
            continue

        next_node = next_active_node()
        if next_node == NODE_ID:
            print(f"[Nó {NODE_ID}] Sou o único nó ativo, mantendo token.", flush=True)
            time.sleep(1)
            continue

        addr = f"tcp://cluster-node-{next_node}.cluster-node-service.default.svc.cluster.local:{HEARTBEAT_PORT}"
        
        with context.socket(zmq.REQ) as next_socket:
            next_socket.setsockopt(zmq.SNDTIMEO, 2000)
            next_socket.setsockopt(zmq.RCVTIMEO, 2000)
            next_socket.connect(addr)
            
            try:
                with active_nodes_lock:
                    current_state = active_nodes.copy()
                next_socket.send_pyobj(current_state)
                next_socket.recv_pyobj()
                
                with active_nodes_lock:
                    active_nodes[next_node] = (True, -1)
                print(f"[Nó {NODE_ID}] Nó {next_node} está ativo.", flush=True)
                
                send_addr = f"tcp://cluster-node-{next_node}.cluster-node-service.default.svc.cluster.local:{HEARTBEAT_TOKEN_PORT}"
                with context.socket(zmq.REQ) as send_socket:
                    send_socket.setsockopt(zmq.SNDTIMEO, 2000)
                    send_socket.connect(send_addr)
                    with active_nodes_lock:
                        send_socket.send_pyobj(active_nodes)
                    send_socket.recv_pyobj()
                    print(f"[Nó {NODE_ID}] Enviou token de verificação para nó {next_node}", flush=True)
                    
            except zmq.ZMQError as e:
                with active_nodes_lock:
                    active_nodes[next_node] = (False, int(time.time() * 1000))
                print(f"[Nó {NODE_ID}] Nó {next_node} não respondeu: {e}, marcando como inativo.", flush=True)

def token_exists():
    """Verifica se o token de verificação já foi recebido recentemente."""
    with active_nodes_lock:
        # Se algum nó tem timestamp recente (menos de 10 segundos), assume que o token existe
        current_time = int(time.time() * 1000)
        for active, timestamp in active_nodes:
            if not active and timestamp != -1 and (current_time - timestamp) < 10000:
                return True
    return False

def heartbeat_receiver_thread():
    socket = context.socket(zmq.REP)
    socket.bind(MY_HEARTBEAT_ADDR)
    
    while True:
        try:
            msg = socket.recv_pyobj()
            socket.send_pyobj(msg)
        except zmq.ZMQError as e:
            print(f"[Nó {NODE_ID}] Erro no heartbeat receiver: {e}", flush=True)
            time.sleep(1)

def build_token():
    with token_copy_lock:
        if token_copy is not None:
            return token_copy.copy()
    return [("", -1)] * TOTAL_NODES

def recreate_token():
    global active_nodes
    token = build_token()
    next_node = next_active_node()
    
    socket = context.socket(zmq.PUSH)
    socket.connect(f"tcp://cluster-node-{next_node}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}")
    socket.setsockopt(zmq.SNDTIMEO, 2000)
    
    try:
        socket.send_pyobj(token)
        print(f"[Nó {NODE_ID}] Token recriado e enviado para nó {next_node}", flush=True)
    except zmq.ZMQError:
        print(f"[Nó {NODE_ID}] Falha ao recriar token para nó {next_node}", flush=True)

def recreate_verification_token():
    global active_nodes
    with active_nodes_lock:
        nodes = active_nodes.copy()
    
    next_node = next_active_node()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://cluster-node-{next_node}.cluster-node-service.default.svc.cluster.local:{HEARTBEAT_TOKEN_PORT}")
    socket.setsockopt(zmq.SNDTIMEO, 2000)
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    
    try:
        socket.send_pyobj(nodes)
        socket.recv_pyobj()
        print(f"[Nó {NODE_ID}] Token de verificação recriado e enviado para nó {next_node}", flush=True)
    except zmq.ZMQError as e:
        print(f"[Nó {NODE_ID}] Falha ao recriar token de verificação: {e}", flush=True)

def next_active_node():
    global active_nodes
    with active_nodes_lock:
        for i in range(1, TOTAL_NODES):
            node = (NODE_ID + i) % TOTAL_NODES
            if active_nodes[node][0]:
                return node
    return NODE_ID

def client_receiver_thread(node_id):
    socket = context.socket(zmq.PULL)
    socket.bind(CLIENT_RECEIVE)
    
    while True:
        try:
            msg = socket.recv_pyobj()
            threading.Thread(target=save_on_buffer_thread, args=(msg,), daemon=True).start()
        except zmq.ZMQError as e:
            print(f"[Nó {NODE_ID}] Erro ao receber mensagem do cliente: {e}", flush=True)
            time.sleep(1)

def client_sender_thread():
    while True:
        if not processed_buffer.empty():
            msg = processed_buffer.get()
            socket = context.socket(zmq.PUSH)
            socket.connect(CLIENT_SEND)
            socket.setsockopt(zmq.SNDTIMEO, 2000)
            
            try:
                socket.send_pyobj(msg)
                print(f"[Nó {NODE_ID}] Enviou resposta para o cliente: {msg[0]}", flush=True)
            except zmq.ZMQError as e:
                print(f"[Nó {NODE_ID}] Erro ao enviar para cliente: {e}", flush=True)
                processed_buffer.put(msg)  # Reenfileira em caso de falha

def token_receiver_thread():
    socket = context.socket(zmq.PULL)
    socket.bind(MY_TOKEN_ADDR)
    socket.setsockopt(zmq.RCVTIMEO, HEARTBEAT_TIMEOUT * 1000)
    
    print(f"[Nó {NODE_ID}] Aguardando token inicial...", flush=True)
    time.sleep(20)  # Sincronização inicial
    
    while True:
        try:
            token = socket.recv_pyobj(flags=zmq.NOBLOCK)
            print(f"[Nó {NODE_ID}] Recebeu token: {token}", flush=True)
            
            if token[NODE_ID][1] != -1:
                is_smallest = all(t[1] == -1 or t[1] > token[NODE_ID][1] for t in token)
                if is_smallest:
                    print(f"[Nó {NODE_ID}] Entrando na zona crítica", flush=True)
                    threads = []
                    
                    threads.append(threading.Thread(target=lambda: processed_buffer.put(request_processing_thread(token[NODE_ID]))))
                    while not received_buffer.empty():
                        request = received_buffer.get()
                        threads.append(threading.Thread(target=lambda r=request: processed_buffer.put(request_processing_thread(r))))
                    
                    for t in threads:
                        t.start()
                    for t in threads:
                        t.join()
                    
                    token[NODE_ID] = ("", -1)
                    print(f"[Nó {NODE_ID}] Saindo da zona crítica", flush=True)
            
            if not received_buffer.empty() and token[NODE_ID][1] == -1:
                token[NODE_ID] = received_buffer.get()
            
            with token_copy_lock:
                global token_copy
                token_copy = token.copy()
            
            next_node = next_active_node()
            next_socket = context.socket(zmq.PUSH)
            next_socket.connect(f"tcp://cluster-node-{next_node}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}")
            next_socket.setsockopt(zmq.SNDTIMEO, 5000)
            next_socket.send_pyobj(token)
            print(f"[Nó {NODE_ID}] Enviou token para nó {next_node}", flush=True)
            
        except zmq.Again:
            if NODE_ID == 0 and not token_exists():
                print(f"[Nó {NODE_ID}] Timeout ao receber token, recriando...", flush=True)
                recreate_token()
            else:
                print(f"[Nó {NODE_ID}] Timeout ao receber token, aguardando...", flush=True)
            time.sleep(5)
        
def token_creator_thread():
    if NODE_ID == 0:
        time.sleep(10)  # Aguarda inicialização
        token = [("", -1)] * TOTAL_NODES
        
        socket = context.socket(zmq.PUSH)
        socket.connect(NEXT_TOKEN_ADDR)
        socket.setsockopt(zmq.SNDTIMEO, 5000)
        
        for _ in range(3):
            try:
                socket.send_pyobj(token)
                print(f"[Nó {NODE_ID}] Token criado e enviado para {NEXT_TOKEN_ADDR}", flush=True)
                break
            except zmq.ZMQError as e:
                print(f"[Nó {NODE_ID}] Falha ao enviar token inicial: {e}", flush=True)
                time.sleep(2)
        
        # Inicializa o token de verificação
        socket_alive = context.socket(zmq.REQ)
        socket_alive.connect(f"tcp://cluster-node-{(NODE_ID + 1) % TOTAL_NODES}.cluster-node-service.default.svc.cluster.local:{HEARTBEAT_TOKEN_PORT}")
        socket_alive.setsockopt(zmq.SNDTIMEO, 2000)
        with active_nodes_lock:
            socket_alive.send_pyobj(active_nodes)
            socket_alive.recv_pyobj()
        print(f"[Nó {NODE_ID}] Token de verificação inicial enviado", flush=True)

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

# Iniciar threads
threads = [
    threading.Thread(target=client_receiver_thread, args=(NODE_ID,), daemon=True),
    threading.Thread(target=client_sender_thread, daemon=True),
    threading.Thread(target=token_receiver_thread, daemon=True),
    threading.Thread(target=token_creator_thread, daemon=True),
    threading.Thread(target=detect_active_nodes, daemon=True),
    threading.Thread(target=heartbeat_receiver_thread, daemon=True),
]

for t in threads:
    t.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(f"[Nó {NODE_ID}] Encerrando...", flush=True)