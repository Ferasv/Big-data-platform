from confluent_kafka import Producer, KafkaException
import socket
import random
import time

def is_reachable(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except:
        return False


# # Cluster A
# CLUSTER_A = {"bootstrap.servers": "localhost:9092"}            # example
# A_HOST = "localhost"
# A_PORT = 9092

# Cluster B
# CLUSTER_B = {"bootstrap.servers": "192.168.126.48:9092"}       # example
# B_HOST = "192.168.126.48"
# B_PORT = 9092



CLUSTER_B = {"bootstrap.servers": "192.168.126.51:9092"}       # example
B_HOST = "192.168.126.51"
B_PORT = 9092



def get_active_producer():
    """Return a producer for the first available cluster."""
    
    # if is_reachable(A_HOST, A_PORT):
    #     print("Using Cluster A")
    #     return Producer(CLUSTER_A)
    
    if is_reachable(B_HOST, B_PORT):
        print("Using Cluster B")
        return Producer(CLUSTER_B)
    
    raise RuntimeError("No Kafka cluster available")


def send_message(topic, value):
    """Send message using the first available cluster."""
    p = get_active_producer()
    try:
        p.produce(topic, value.encode("utf-8"))
        p.flush()
    except KafkaException as e:
        print(f"Kafka error: {e}")


# Example usage

while True:
    random_number = random.randint(1, 100)
    send_message("my_first_topic", str(random_number))
    time.sleep(8)
    
    print("Message sent")
