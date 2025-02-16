import threading
import zmq
import time

# Configuração do cliente
NODE_ID = 1  # O ID do nó que o cliente irá se comunicar (ex: 1, 2, 3)
CLIENT_PORT = 7000  # Porta base para comunicação com clientes
CLIENT_RECEIVE = f"tcp://127.0.0.1:{CLIENT_PORT + NODE_ID + 100}"
CLIENT_SEND = f"tcp://127.0.0.1:{CLIENT_PORT + NODE_ID}"

# Contexto do ZeroMQ
context = zmq.Context()

# Enviar pedido para o nó
def send_request():
    socket = context.socket(zmq.PUSH)
    socket.connect(CLIENT_SEND)  # Conecta ao nó (portas definidas no nó)
    
    while True:
        timestamp = int(time.time() * 1000)  # timestamp em milissegundos
        msg = (f"Pedido de saldo do cliente {NODE_ID}", timestamp)  # inclui o timestamp na mensagem
        socket.send_pyobj(msg)
        print(f"[Cliente 1] {msg[1]} Enviou: {msg[0]}")
        time.sleep(3)

#  Receber resposta do nó
def receive_response():
    socket = context.socket(zmq.PULL)
    socket.connect(CLIENT_RECEIVE)  # Conecta ao nó (respostas serão enviadas para essa porta)
    
    while True:
        response = socket.recv_pyobj()
        print(f"[Cliente 1] Resposta recebida: {response[0]}| timestamp: {response[1]}")

# Criar e iniciar as threads para enviar e receber mensagens
send_thread = threading.Thread(target=send_request, daemon=True)
receive_thread = threading.Thread(target=receive_response, daemon=True)

send_thread.start()
receive_thread.start()

# Mantém o programa rodando
send_thread.join()
receive_thread.join()
