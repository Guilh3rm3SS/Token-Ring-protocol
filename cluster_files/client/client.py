import re
import socket
import threading
import zmq
import time

# Configuração do cliente
def get_pod_id():
    hostname = socket.gethostname()  # Obtém o nome do host (nome do pod)
    match = re.search(r'client-node-(\d+)', hostname)  # Captura o número após "cluster-node-"
    return int(match.group(1)) if match else -1  # Retorna o número extraído ou -1 se não encontrar

NODE_ID = get_pod_id()  # ID do nó atual

CLIENT_PORT = 7000  # Porta base para comunicação com clientes
NODE_PORT = 7000

CLIENT_RECEIVE = f"tcp://client-node-{NODE_ID}.client-service.default.svc.cluster.local:{CLIENT_PORT}"

CLIENT_SEND = f"tcp://cluster-node-{NODE_ID}.cluster-node-service.default.svc.cluster.local:{CLIENT_PORT}"

# Contexto do ZeroMQ
context = zmq.Context()

# Enviar pedido para o nó
def send_request():
    socket = context.socket(zmq.PUSH)
    socket.connect(CLIENT_SEND)  # Conecta ao nó (portas definidas no nó)
    count = 0
    while True:
        count += 1
        timestamp = int(time.time() * 1000)  # timestamp em milissegundos
        msg = (f"Pedido {count} cliente {NODE_ID}", timestamp)  # inclui o timestamp na mensagem
        socket.send_pyobj(msg)
        print(f"[Cliente {NODE_ID}] Enviou: pedido {count} | timestamp: {msg[1]}", flush=True)
        time.sleep(3)

#  Receber resposta do nó
def receive_response():
    socket = context.socket(zmq.PULL)
    socket.bind(CLIENT_RECEIVE)  # Conecta ao nó (respostas serão enviadas para essa porta)
    
    while True:
        response = socket.recv_pyobj()
        print(f"[Cliente {NODE_ID}] Resposta recebida: {response[0]} concluído | timestamp: {response[1]}", flush=True)

# Criar e iniciar as threads para enviar e receber mensagens
send_thread = threading.Thread(target=send_request, daemon=True)
receive_thread = threading.Thread(target=receive_response, daemon=True)

send_thread.start()
receive_thread.start()

# Mantém o programa rodando
send_thread.join()
receive_thread.join()
