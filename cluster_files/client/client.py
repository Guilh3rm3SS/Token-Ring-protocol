import zmq
import threading
import time
import os
from datetime import datetime


NODE_ID = int(os.getenv("NODE_ID", -1))

CLIENT_PORT = 7000
NODE_PORT = 7000

CLIENT_RECEIVE = f"tcp://*:{CLIENT_PORT + NODE_ID}"
CLIENT_SEND = f"tcp://cluster-node-{NODE_ID}:{CLIENT_PORT + NODE_ID}"

context = zmq.Context()

confirmation_event = threading.Event()
message_lock = threading.Lock()
last_message = None
timeout = 1000000000  # segundos para timeout

def send_request():
    global last_message
    socket = context.socket(zmq.PUSH)
    socket.connect(CLIENT_SEND)
    count = 0

    while True:
        with message_lock:
            count += 1
            timestamp = int(time.time() * 1000)
            last_message = (f"Pedido {count} cliente {NODE_ID}", timestamp, NODE_ID, count)

        while True:
            # Envia mensagem
            socket.send_pyobj(last_message)
            hora_atual = datetime.now().strftime("%H:%M:%S")
            print(f"\033[94m[{hora_atual}] [Cliente {NODE_ID}] 📤 Enviou: pedido {count} | timestamp: {last_message[1]}\033[0m", flush=True)

            # Aguarda confirmação com timeout
            if confirmation_event.wait(timeout):
                # Confirmação recebida
                confirmation_event.clear()
                break
            else:
                # Timeout: reenviar
                hora_atual = datetime.now().strftime("%H:%M:%S")
                print(f"\033[93m[{hora_atual}] [Cliente {NODE_ID}] ⏰ Timeout! Reenviando pedido {count}\033[0m", flush=True)

def receive_response():
    socket = context.socket(zmq.PULL)
    socket.bind(CLIENT_RECEIVE)

    global last_message
    while True:
        response = socket.recv_pyobj()
        if response is not None:
            with message_lock:
                # Checa se a resposta é para o último pedido enviado
                if response[3] == last_message[3]:
                    hora_atual = datetime.now().strftime("%H:%M:%S")
                    print(f"\033[92m[{hora_atual}] [Cliente {NODE_ID}] ✅ Resposta recebida: {response[0]} concluído | timestamp: {response[1]}\033[0m", flush=True)
                    confirmation_event.set()
        else:
            print(f"\033[91m[Cliente {NODE_ID}] ❌ Erro: resposta vazia ou conexão encerrada\033[0m", flush=True)

# Iniciar threads
send_thread = threading.Thread(target=send_request, daemon=True)
receive_thread = threading.Thread(target=receive_response, daemon=True)

time.sleep(10)  # Espera inicial
send_thread.start()
receive_thread.start()

send_thread.join()
receive_thread.join()