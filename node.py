import zmq
import threading
import time
import random
import queue

# Configuração da rede
TOKEN_PORT = 6000  # Porta base para comunicação entre nós
CLIENT_PORT = 7000  # Porta base para comunicação com clientes

NODE_ID = 0  # ID do nó atual
TOTAL_NODES = 2  # Número total de nós na rede

# Endereços para comunicação entre nós
PREVIOUS_NODE = f"tcp://127.0.0.1:{TOKEN_PORT + (NODE_ID - 1) % TOTAL_NODES}"
NEXT_NODE = f"tcp://127.0.0.1:{TOKEN_PORT + NODE_ID}"

# Endereços para comunicação com clientes
CLIENT_RECEIVE = f"tcp://127.0.0.1:{CLIENT_PORT + NODE_ID}"
CLIENT_SEND = f"tcp://127.0.0.1:{CLIENT_PORT + NODE_ID + 100}"

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
        print(f"[Nó {NODE_ID}] Recebeu pedido do cliente: {msg[0]} | timestamp: {msg[1]}")
        received_buffer.put(msg)

# Enviar respostas aos clientes
def client_sender_thread():
    socket = context.socket(zmq.PUSH)
    socket.bind(CLIENT_SEND)

    while True:
        #time.sleep(5)
        if not processed_buffer.empty():
            msg = processed_buffer.get()
            
            #msg = f"Resposta do nó {NODE_ID}"
            socket.send_pyobj(msg)
            print(f"[Nó {NODE_ID}] Enviou resposta para o cliente: {msg[0]}")

# Receber o token do nó anterior
def token_receiver_thread():
    socket = context.socket(zmq.PULL)
    socket.bind(PREVIOUS_NODE)
    
    while True:
        token = socket.recv_pyobj()
        print(f"[Nó {NODE_ID}] Recebeu token {token}")
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
                processed_buffer.put(token[NODE_ID])
                print(f"Token processado {token[NODE_ID][1]}")
                token[NODE_ID] = ("", -1)
                print(f"Após inserir nulo, token:{token}")
                time.sleep(random.uniform(0.2, 1.0)) # Simula tempo de processamento
        
        # Simula acesso ao recurso na zona crítica
        if not received_buffer.empty() and token[NODE_ID][1] == -1:
            token[NODE_ID] = received_buffer.get()
        
        # Passa o token para o próximo nó
        next_socket = context.socket(zmq.PUSH)
        next_socket.connect(NEXT_NODE)
        next_socket.send_pyobj(token)
        print(f"[Nó {NODE_ID}] Enviou token para o próximo nó")

# Enviar o token para o próximo nó
def token_creator_thread():
    socket = context.socket(zmq.PUSH)
    socket.connect(NEXT_NODE)

    if NODE_ID == 0:
        # O nó inicializa o token
        time.sleep(1)
        token = [("", -1)] * TOTAL_NODES
        
        socket.send_pyobj(token)
        print(f"[Nó {NODE_ID}] Iniciou o token")

# Criar e iniciar as threads
threads = [
    threading.Thread(target=client_receiver_thread, daemon=True),
    threading.Thread(target=client_sender_thread, daemon=True),
    threading.Thread(target=token_receiver_thread, daemon=True),
    threading.Thread(target=token_creator_thread, daemon=True)
]

for t in threads:
    t.start()

# Mantém o programa rodando
for t in threads:
    t.join()
