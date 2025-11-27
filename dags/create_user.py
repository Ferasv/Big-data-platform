import random
from faker import Faker
import uuid
import time
from datetime import datetime

fake = Faker()

def generate_random_user():
    user_id = str(uuid.uuid4())
    name = fake.name()
    phone = fake.phone_number()
    created_at = datetime.utcnow().isoformat()
    price = round(random.uniform(5.0, 140.0), 2)

    return {
        "name": name,
        "phone": phone,
        "wallet": price,
        
    }
def generate_random_driver():
    driver_id = str(uuid.uuid4())
    name = fake.name()
    phone = fake.phone_number()
    vehicle_type = fake.random_element(elements=["Car", "Bike", "Scooter"])
    created_at = datetime.utcnow().isoformat()
    

    return {
        "name": name,
        "phone": phone,
        "status": "available",
        "vehicle_type": vehicle_type,
        "wallet": 0
        
    }

def main():
    user = generate_random_user()
    driver = generate_random_driver()
    
    # print(f"Generated User: {user}")
    # print(f"Generated Driver: {driver}")
    time.sleep(5)
    
    return user, driver
