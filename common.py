import pika, json, os, sys, psycopg2
from dataclasses import dataclass

@dataclass
class Telegram:
    queue: str
    nation: str
    tgid: str
    tg_key: str
    client_key: str

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, separators=(',', ':'))

def send_telegrams(telegrams: list[Telegram]):
    for telegram in telegrams:
        print(telegram.toJSON())

def actual_send_telegrams(telegrams: list[Telegram]):
    url = os.getenv("RABBITMQ_URL")
    if url is None:
        print("RABBITMQ_URL not provided in the environment!", file=sys.stderr)
        sys.exit(1)

    connection = pika.BlockingConnection(pika.URLParameters(url))
    channel = connection.channel()

    channel.queue_declare(queue='crystal_server')

    for telegram in telegrams:
         channel.basic_publish(exchange='',
                        routing_key='crystal_server',
                        body=telegram.toJSON())
    
    connection.close()

def open_db():
    url = os.getenv("DATABASE_URL")
    if url is None:
        print("DATABASE_URL not provided in the environment!", file=sys.stderr)
        sys.exit(1)

    return psycopg2.connect(url)