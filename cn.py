import zmq
import socket
import re
import threading
import time
import random
import queue

# Configuração da rede
TOKEN_PORT = 6000  # Porta base para comunicação entre nós
CLIENT_PORT = 7000  # Porta base para comunicação com clientes

def get_pod_id():
    hostname = socket.gethostname()  # Obtém o nome do host (nome do pod)
    match = re.search(r'cluster-node-(\d+)', hostname)  # Captura o número após "cluster-node-"
    return int(match.group(1)) if match else -1  # Retorna o número extraído ou -1 se não encontrar

NODE_ID = get_pod_id()  # ID do nó atual
TOTAL_NODES = 5  # Número total de nós na rede

# Endereços para comunicação entre nós
MY_TOKEN_ADDR = f"tcp://cluster-node-{NODE_ID}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"
NEXT_NODE = f"tcp://cluster-node-{(NODE_ID + 1) % TOTAL_NODES}.cluster-node-service.default.svc.cluster.local:{TOKEN_PORT}"



# Endereços para comunicação com clientes
CLIENT_RECEIVE = f"tcp://cluster-node-{NODE_ID}.cluster-node-service.default.svc.cluster.local:{CLIENT_PORT}"
CLIENT_SEND = f"tcp://client-node-{NODE_ID}.client-service.default.svc.cluster.local:{CLIENT_PORT}"

# Contexto do ZeroMQ
context = zmq.Context()
received_buffer = queue.Queue()
processed_buffer = queue.Queue()


#  Receber pedidos dos clientes
def client_receiver_thread():
    socket = context.socket(zmq.PULL)
    socket.bind(CLIENT_RECEIVE)
    
    while True:
        msg = socket.recv_pyobj()
        threading.Thread(target=save_on_buffer_thread, args=(msg))

# Enviar respostas aos clientes
def client_sender_thread():
    socket = context.socket(zmq.PUSH)
    socket.connect(CLIENT_SEND)

    while True:
        #time.sleep(5)
        if not processed_buffer.empty():
            msg = processed_buffer.get()
            
            #msg = f"Resposta do nó {NODE_ID}"
            socket.send_pyobj(msg)
            print(f"[Nó {NODE_ID}] Enviou resposta para o cliente: {msg[0]}", flush=True)

# Receber o token do nó anterior
def token_receiver_thread():
    time.sleep(1)
    socket = context.socket(zmq.PULL)
    socket.bind(MY_TOKEN_ADDR)
    
    while True:
        token = socket.recv_pyobj()
        print(f"[Nó {NODE_ID}] Recebeu token", flush=True)
        time.sleep(random.uniform(0.2, 1.0))
        # Simula uso do recurso
        if token[NODE_ID][1] != -1:
            # Processa
            is_smallest = True
            
            # Verifica se o nó atual é o menor
            for token_msg in token:
                if token_msg[1] != -1 and token_msg[1] < token[NODE_ID][1]:
                    is_smallest = False
                    break
            
            # Se for menor, processa
            if is_smallest:
                
                print(f"[Nó {NODE_ID}] Entrando na zona crítica", flush=True)
                
                threads = []

                # Adiciona a thread principal (token[NODE_ID])
                threads.append(threading.Thread(target=request_processing_thread(token[NODE_ID])))

                # Adiciona threads para cada item na received_buffer
                while not received_buffer.empty():
                    request = received_buffer.get()
                    threads.append(threading.Thread(target=lambda req=request: processed_buffer.put(request_processing_thread(req))))

                # Inicia todas as threads
                for t in threads:
                    t.start()

                # Aguarda todas as threads terminarem
                for t in threads:
                    t.join()

                # Atualiza o token
                token[NODE_ID] = ("", -1)
                
                print(f"[Nó {NODE_ID}] Saindo da zona crítica", flush=True)
        
        # Simula acesso ao recurso na zona crítica
        if not received_buffer.empty() and token[NODE_ID][1] == -1:
            token[NODE_ID] = received_buffer.get()
        
        # Passa o token para o próximo nó
        next_socket = context.socket(zmq.PUSH)
        next_socket.connect(NEXT_NODE)
        next_socket.send_pyobj(token)
        print(f"[Nó {NODE_ID}] Enviou token para o próximo nó", flush=True)

# Enviar o token para o próximo nó
def token_creator_thread():
    if NODE_ID == 4:
        
        time.sleep(2)
        socket = context.socket(zmq.PUSH)
        socket.connect(NEXT_NODE)

        # O nó inicializa o token
        print(f"[Nó {NODE_ID}] Criou o token", flush=True)
        time.sleep(1)
        token = [("", -1)] * TOTAL_NODES
        
        socket.send_pyobj(token)
        print(f"[Nó {NODE_ID}] Iniciou o token", flush=True)
        

def request_processing_thread(message):
    time.sleep(random.uniform(0.2, 1.0))
    print(f"[Nó {NODE_ID}] Requisição processada {message[1]}", flush=True)
    processed_buffer.put(message)
    
def save_on_buffer_thread(message):
    if message is not None:
            print(f"[Nó {NODE_ID}] Recebeu pedido do cliente: {message[0]} | timestamp: {message[1]}", flush=True)
            received_buffer.put(message)
        
    else: 
        print(f"[Nó {NODE_ID}] Erro ao receber pedido", flush=True)
    
        

# Criar e iniciar as threads
threads = [
    threading.Thread(target=client_receiver_thread, daemon=True),
    threading.Thread(target=client_sender_thread, daemon=True),
    threading.Thread(target=token_receiver_thread, daemon=True),
    threading.Thread(target=token_creator_thread, daemon=True)
]

time.sleep(10)
for t in threads:
    t.start()

# Mantém o programa rodando
for t in threads:
    t.join()
