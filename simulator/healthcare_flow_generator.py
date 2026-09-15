import json
import random
import uuid
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer


# ============================================================
# Event Hub Configuration
# ============================================================

EVENTHUBS_NAMESPACE = "<<NAMESPACE_HOSTNAME>>"
EVENT_HUB_NAME = "<<EVENT_HUB_NAME>>"
CONNECTION_STRING = "<<NAMESPACE_CONNECTION_STRING>>"


producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ============================================================
# Healthcare Data
# ============================================================

hospitals = [
    "Hospital Central",
    "Hospital San Rafael",
    "Hospital Metropolitano",
    "Hospital Nacional",
    "Hospital Santa Maria"
]

departments = [
    "Emergency",
    "Surgery",
    "ICU",
    "Pediatrics",
    "Maternity",
    "Oncology",
    "Cardiology",
    "Neurology",
    "Orthopedics"
]

genders = [
    "Male",
    "Female"
]

diagnoses = [
    "Diabetes",
    "Hypertension",
    "Asthma",
    "Pneumonia",
    "Heart Disease",
    "Cancer",
    "Arthritis",
    "COPD",
    "Kidney Disease"
]


# ============================================================
# Dirty Data Injection
# ============================================================

def inject_dirty_data(record):

    # 5% chance of invalid age
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)

    # 5% chance of future admission timestamp
    if random.random() < 0.05:
        record["admission_time"] = (
            datetime.utcnow()
            + timedelta(hours=random.randint(1, 72))
        ).isoformat()

    # 3% chance of missing gender
    if random.random() < 0.03:
        record["gender"] = None

    # 3% chance of missing diagnosis
    if random.random() < 0.03:
        record["diagnosis"] = None

    # 3% chance of negative claim cost
    if random.random() < 0.03:
        record["claim_cost"] = -random.randint(100, 5000)

    return record


# ============================================================
# Generate Patient Event
# ============================================================

def generate_patient_event():

    admission_time = (
        datetime.utcnow()
        - timedelta(hours=random.randint(0, 72))
    )

    length_of_stay = random.randint(1, 10)

    discharge_time = (
        admission_time
        + timedelta(days=length_of_stay)
    )

    event = {

        "event_id": str(uuid.uuid4()),

        "patient_id": str(uuid.uuid4()),

        "hospital_id": random.randint(1, 5),

        "hospital_name": random.choice(hospitals),

        "gender": random.choice(genders),

        "age": random.randint(1, 100),

        "department": random.choice(departments),

        "diagnosis": random.choice(diagnoses),

        "admission_time": admission_time.isoformat(),

        "discharge_time": discharge_time.isoformat(),

        "length_of_stay_days": length_of_stay,

        "readmitted": random.choice([True, False]),

        "claim_cost": round(
            random.uniform(500, 15000),
            2
        ),

        "event_timestamp": datetime.utcnow().isoformat()
    }

    return inject_dirty_data(event)


# ============================================================
# Producer
# ============================================================

if __name__ == "__main__":

    while True:

        event = generate_patient_event()

        producer.send(
            EVENT_HUB_NAME,
            event
        )

        print(
            f"Sent patient event to Event Hub: {event}"
        )

        time.sleep(1)