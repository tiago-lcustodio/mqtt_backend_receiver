import time
import json
import random
import threading
import paho.mqtt.client as mqtt

# --- CONFIGURAÇÕES DO BARRAMENTO (Alinhado com config.go) ---
BROKER_HOST = "localhost"
BROKER_PORT = 1884  # Porta dedicada para o fluxo downstream
TOPICS = [
    "unioeste/iot/receiver",
    "unioeste/iot/receiver/replica-1",
    "unioeste/iot/receiver/replica-2",
    "unioeste/iot/receiver/replica-3"
]

# --- ESTADOS OPERACIONAIS DA BANCADA ---
# NORMAL: Funciona perfeitamente
# OUTAGE: Ignora conexões/mensagens totalmente para forçar time-out físico
# SLOW: Introduz delay artificial de 2.5s (Estoura o threshold de 2s)
# LOSSY: Dropa 35% das mensagens de forma probabilística
CURRENT_MODE = "NORMAL"

# --- MÉTRICAS COLETADAS PELO RECEIVER ---
metrics = {
    "total_received": 0,
    "normal_received": 0,
    "replicas_received": 0,
    "processed_ids": set(),
    "duplicated_count": 0
}
metrics_lock = threading.Lock()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"\n[RECEIVER] Conectado ao Broker Downstream ({BROKER_HOST}:{BROKER_PORT})")
        for topic in TOPICS:
            client.subscribe(topic, qos=1)
            print(f"[RECEIVER] Subscrito no tópico: {topic}")
        print_menu()
    else:
        print(f"[RECEIVER] [ERRO] Falha na conexão. Código de retorno: {rc}")

def on_message(client, userdata, msg):
    global CURRENT_MODE
    
    # 1. Se estiver em modo OUTAGE, o script ignora o processamento (simula travamento total de socket)
    if CURRENT_MODE == "OUTAGE":
        return

    # 2. Se estiver em modo SLOW_CONSUMER, induz lentidão superior ao threshold do Go (2 segundos)
    if CURRENT_MODE == "SLOW":
        print(f"[RECEIVER] [SLOW] Mensagem recebida. Segurando ACK por 2.5 segundos...")
        time.sleep(2.5)

    # 3. Se estiver em modo LOSSY_LINK, simula perda de pacotes na camada de aplicação (dropa 35%)
    if CURRENT_MODE == "LOSSY":
        if random.random() < 0.35:
            print(f"[RECEIVER] [LOSSY] [DROP] Simulando perda de pacote. Descartando mensagem silenciosamente.")
            return

    # 4. Processamento legítimo da mensagem IoT
    with metrics_lock:
        metrics["total_received"] += 1
        
        # Identifica se é fluxo padrão ou réplica ativa
        if "replica" in msg.topic:
            metrics["replicas_received"] += 1
        else:
            metrics["normal_received"] += 0 # Fluxo nominal

        try:
            # Tenta decodificar o payload vindo do TTS/Middleware
            payload_str = msg.payload.decode('utf-8')
            
            # Checagem de duplicação para validar estatisticamente a Replicação Ativa
            # Como o payload do TTS pode conter campos mapeáveis, tentamos rastrear IDs únicos
            if "msg_id" in payload_str or "id" in payload_str:
                try:
                    data = json.loads(payload_str)
                    msg_id = data.get("id") or data.get("msg_id")
                    if msg_id:
                        if msg_id in metrics["processed_ids"]:
                            metrics["duplicated_count"] += 1
                            print(f"[RECEIVER] [DUPLICADA] Réplica detectada para o ID: {msg_id}")
                        else:
                            metrics["processed_ids"].add(msg_id)
                except:
                    pass

            print(f"[RECEIVER] [{CURRENT_MODE}] Mensagem recebida com sucesso! Tópico: {msg.topic} | Tamanho: {len(msg.payload)} bytes")
        except Exception as e:
            print(f"[RECEIVER] Erro ao ler payload: {e}")

def print_menu():
    print("\n" + "="*50)
    print(f" CONTROLE DA BANCADA DE TESTES - MODO ATUAL: [{CURRENT_MODE}]")
    print("="*50)
    print(" [1] Alterar para modo: NORMAL (Respostas estáveis)")
    print(" [2] Alterar para modo: OUTAGE (Simular queda total)")
    print(" [3] Alterar para modo: SLOW CONSUMER (Latência de 2.5s)")
    print(" [4] Alterar para modo: LOSSY LINK (Perda randômica de 35%)")
    print(" [5] Exibir Relatório de QoS (Dados salvos / duplicados)")
    print(" [0] Encerrar o Receiver")
    print("="*50)
    print("Escolha uma opção: ", end="")

def interactive_menu():
    global CURRENT_MODE
    while True:
        try:
            choice = input().strip()
            if choice == "1":
                CURRENT_MODE = "NORMAL"
                print(f"\n[BANCADA] Alterado para NORMAL. Sistema operacional limpo.")
                print_menu()
            elif choice == "2":
                CURRENT_MODE = "OUTAGE"
                print(f"\n[BANCADA] Alterado para OUTAGE. Simulando indisponibilidade do nó.")
                print_menu()
            elif choice == "3":
                CURRENT_MODE = "SLOW"
                print(f"\n[BANCADA] Alterado para SLOW CONSUMER. Forçando gargalo de processamento.")
                print_menu()
            elif choice == "4":
                CURRENT_MODE = "LOSSY"
                print(f"\n[BANCADA] Alterado para LOSSY LINK. Forçando descarte probabilístico.")
                print_menu()
            elif choice == "5":
                with metrics_lock:
                    print("\n" + "-"*40)
                    print("         RELATÓRIO DE QoS DO RECEIVER")
                    print("-"*40)
                    print(f" Total de Mensagens Processadas: {metrics['total_received']}")
                    print(f" Mensagens via Canal Nominal:   {metrics['total_received'] - metrics['replicas_received']}")
                    print(f" Mensagens via Réplicas Ativas: {metrics['replicas_received']}")
                    print(f" Mensagens Duplicadas Físicas:  {metrics['duplicated_count']}")
                    print(f" IDs Únicos Preservados:        {len(metrics['processed_ids'])}")
                    print("-"*40)
                print_menu()
            elif choice == "0":
                print("\n[RECEIVER] Desligando bancada de testes...")
                break
            else:
                print("Opção inválida. Digite de 0 a 5: ", end="")
        except KeyboardInterrupt:
            break

def main():
    # Inicializa o cliente MQTT usando a especificação estável v3.1.1 do Paho
    client = mqtt.Client(client_id="unioeste_receiver_node")
    client.on_connect = on_connect
    client.on_message = on_message

    # Tenta se conectar de forma resiliente
    try:
        client.connect(BROKER_HOST, BROKER_PORT, 60)
    except Exception as e:
        print(f"[RECEIVER] [ERRO] Não foi possível conectar ao broker local na porta {BROKER_PORT}: {e}")
        return

    # Inicia o laço de escuta da rede em uma thread de background
    client.loop_start()

    # Inicia o menu interativo na thread principal
    interactive_menu()

    # Finalização limpa
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    main()