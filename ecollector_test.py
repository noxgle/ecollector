from ExFixCollector import *

basedir = os.path.abspath(os.path.dirname(__file__))

def load_key():
    return open("fernet.key", "rb").read()


cipher_suite = Fernet(load_key())


def encrypt_message(message):
    return cipher_suite.encrypt(message.encode())


def decrypt_message(encrypted_message):
    return cipher_suite.decrypt(encrypted_message).decode()


zmq_ip = "127.0.0.1"
zmq_port = 5557

try:
    fix = ExFixCollector()

    time.sleep(3)
    symbol = 'AAPL.NASDAQ'
    context = zmq.Context()
    soc = context.socket(zmq.REQ)
    soc.setsockopt(zmq.LINGER, 0)
    soc.setsockopt(zmq.RCVTIMEO, 5000)
    soc.connect(f"tcp://{zmq_ip}:{zmq_port}")
    soc.send(encrypt_message(json.dumps({"order": "attach", "symbol": symbol})))
    response = json.loads(decrypt_message(soc.recv()))
    print(response)
    soc.close()

    time.sleep(3)
    symbol = 'COKE.NASDAQ'
    context = zmq.Context()
    soc = context.socket(zmq.REQ)
    soc.setsockopt(zmq.LINGER, 0)
    soc.setsockopt(zmq.RCVTIMEO, 5000)
    soc.connect(f"tcp://{zmq_ip}:{zmq_port}")
    soc.send(encrypt_message(json.dumps({"order": "attach", "symbol": symbol})))
    response = json.loads(decrypt_message(soc.recv()))
    print(response)
    soc.close()

    i = 0
    while True:
        if i > 10:
            break
        soc = context.socket(zmq.REQ)
        soc.setsockopt(zmq.LINGER, 0)
        soc.setsockopt(zmq.RCVTIMEO, 5000)
        soc.connect(f"tcp://{zmq_ip}:{zmq_port}")
        soc.send(encrypt_message(json.dumps({"order": "get_data", "symbol": symbol})))
        response = json.loads(decrypt_message(soc.recv()))
        print(response)
        soc.close()
        i += 1
        time.sleep(5)

    soc = context.socket(zmq.REQ)
    soc.setsockopt(zmq.LINGER, 0)
    soc.setsockopt(zmq.RCVTIMEO, 5000)
    soc.connect(f"tcp://{zmq_ip}:{zmq_port}")
    soc.send(encrypt_message(json.dumps({"order": "detach", "symbol": symbol})))
    response = json.loads(decrypt_message(soc.recv()))
    print(response)
    soc.close()

    context.destroy()


except Exception as e:
    print("Fatal error: %s", e)
