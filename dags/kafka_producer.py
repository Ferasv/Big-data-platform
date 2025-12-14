from confluent_kafka import Producer, KafkaException
import socket
import random
import time
from create_user import main
import socket

def is_reachable(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except:
        return False


# Cluster A
CLUSTER_A = {
    "bootstrap.servers": "192.168.126.116:9092",
    "queue.buffering.max.kbytes": "1048576",     # 1GB buffer
    "queue.buffering.max.messages": "1000000",   # message queue
    "linger.ms": "5",                            # micro-batching
    "message.timeout.ms": "60000",               # 60 sec timeout
    "client.id": socket.gethostname(),
}
           # example


# Cluster B
# CLUSTER_B = {"bootstrap.servers": "192.168.126.48:9092"}       # example
# B_HOST = "192.168.126.48"
# B_PORT = 9092



# CLUSTER_B = {"bootstrap.servers": "192.168.126.51:9092"}       # example
# B_HOST = "192.168.126.51"
# B_PORT = 9092






def send_message(topic, value):
    """Send message using the first available cluster."""
    p = Producer(CLUSTER_A)
    try:
        p.produce(topic, value.encode("utf-8"))
        p.flush()
    except KafkaException as e:
        print(f"Kafka error: {e}")


# Example usage
if __name__ == "__main__":
    
    while True:
        user_data,driver = main()    
        send_message("data_user", str(user_data))
        
        
        print("Message sent")
