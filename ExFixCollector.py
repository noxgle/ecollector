import configparser
import json
import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime
from collections import deque
from cryptography.fernet import Fernet
import zmq
import simplefix
import uuid

basedir = os.path.abspath(os.path.dirname(__file__))


def setup_logger(filename=None, logger_name=None, log_to_console=False,
                 file_log_level=logging.INFO, console_log_level=logging.ERROR):
    """
    Setup logging.

    Args:
    - filename (str, optional): If provided, will log to the file.
    - logger_name (str, optional): Name of the logger.
    - log_to_console (bool, optional): If True, will log to console.
    - file_log_level (int, optional): Logging level for file.
    - console_log_level (int, optional): Logging level for console.

    Returns:
    - logger: Configured logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Set to the lowest level, handlers will filter

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if filename:
        fh = logging.FileHandler(filename)
        fh.setLevel(file_log_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    if log_to_console:
        ch = logging.StreamHandler()
        ch.setLevel(console_log_level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


class CommandQueue:
    def __init__(self, max_size):
        self.max_size = max_size
        self.queue = deque(maxlen=max_size)

    def add_command(self, command):
        self.queue.append(command)

    def get_command(self):
        if not self.queue:
            return None
        else:
            return self.queue.popleft()


class FixData:
    def __init__(self, max_size):
        self.max_size = max_size
        self.ask = deque(maxlen=max_size)
        self.bid = deque(maxlen=max_size)
        self.date = deque(maxlen=max_size)

    def add_data(self, ask, bid, date):
        self.ask.append(ask)
        self.bid.append(bid)
        self.date.append(date)

    def get_data(self):
        return list(self.ask), list(self.bid), list(self.date)

    def get_last(self):
        return self.ask[-1], self.bid[-1], self.date[-1]

    def get_first(self):
        return self.ask[0], self.bid[0], self.date[0]


class ExFixCollector:
    def __init__(self, zmq_ip="127.0.0.1", zmq_port=5557, tick_size=1000, fix_conf_file='fix_demo_conf.ini',
                 fernet_key='fernet.key', filename_log='ExFixCollector.log', logger_name='main',
                 file_log_level=logging.DEBUG,
                 console_log_level=logging.ERROR):

        self.filename_log = filename_log
        self.logger_name = logger_name
        self.file_log_level = file_log_level
        self.console_log_level = console_log_level
        self.main_logger = setup_logger(self.filename_log, self.logger_name, file_log_level=self.file_log_level,
                                        console_log_level=self.console_log_level)

        self.attached = {}
        self.terminate = False
        self.status = 0
        self.seq_num = None
        self.sock = None
        self.logged = False
        self.host_fix = None
        self.port_fix = None
        self.fix_version = None
        self.sender_comp_id = None
        self.target_comp_id = None
        self.password = None
        self.fix_tag_price = '270'
        self.tick_size = tick_size
        self.fix_conf_file = fix_conf_file
        self.fernet_key = fernet_key

        self.cmd_queue = CommandQueue(10)
        self.load_config()

        self.zmq_ip = zmq_ip
        self.zmq_port = zmq_port

        self.timestamp_sent = time.time()
        self.timestamp_receive = time.time()

        self.cipher_suite = Fernet(self.load_key())

        self.increment_thread = threading.Thread(target=self.collector)
        self.increment_thread.start()
        self.zmq_thread = threading.Thread(target=self.zmq_server)
        self.zmq_thread.start()

    def load_config(self):
        config = configparser.ConfigParser()
        try:
            config.read(f'{basedir}/{self.fix_conf_file}')
            self.target_comp_id = config['FIX_FEED']['targetcompid']
            self.sender_comp_id = config['FIX_FEED']['sendercompid']
            self.host_fix = config['FIX_FEED']['host']
            self.port_fix = int(config['FIX_FEED']['port'])
            self.password = config['FIX_FEED']['password']
            self.fix_version = config['DEFAULT']['fixversion']

        except Exception as e:
            sys.exit(f'Error reading config file: {self.fix_conf_file}, {e}')

    def load_key(self):
        try:
            r = open(self.fernet_key, "rb").read()
        except Exception as e:
            sys.exit(f'Error reading file: {self.fernet_key}, {e}')
        return r

    def encrypt_message(self, message):
        return self.cipher_suite.encrypt(message.encode())

    def decrypt_message(self, encrypted_message):
        return self.cipher_suite.decrypt(encrypted_message).decode()

    def run(self):
        pass

    def zmq_server(self):
        context = zmq.Context()
        rep_socket = context.socket(zmq.REP)
        try:
            rep_socket.bind(f"tcp://{self.zmq_ip}:{self.zmq_port}")
            self.main_logger.info(f"{self.__class__}: Daemon ZeroMQ server started on port {self.zmq_port}")

            while True:
                message = rep_socket.recv_string()
                try:
                    message = self.decrypt_message(message)
                except Exception as e:
                    self.main_logger.warning(f"{self.__class__}: Decoding message: {e}")
                try:
                    request = json.loads(message)
                except json.JSONDecodeError:
                    self.main_logger.warning(f"{self.__class__}: Incorrect json format")
                    encrypted_text = self.encrypt_message("Incorrect json format")
                    rep_socket.send(encrypted_text)
                else:
                    if 'order' in request:
                        if request['order'] == 'attach' or request['order'] == 'detach':
                            self.cmd_queue.add_command(request)
                            response = json.dumps({"status": "ok"})
                            self.main_logger.debug(f"{self.__class__}: Request: {request} response: {response}")
                            encrypted_text = self.encrypt_message(response)
                            rep_socket.send(encrypted_text)
                            continue
                        elif request['order'] == 'get_data':
                            if request['symbol'] in self.attached:
                                data = self.attached[request['symbol']][1].get_data()
                            else:
                                data = []

                            response = json.dumps({"status": "ok", 'data': data})
                            self.main_logger.debug(f"{self.__class__}: Request: {request} response: {response}")
                            encrypted_text = self.encrypt_message(response)
                            rep_socket.send(encrypted_text)
                            continue
                        elif request['order'] == 'get_last':
                            if request['symbol'] in self.attached:
                                data = self.attached[request['symbol']][1].get_last()
                            else:
                                data = []

                            response = json.dumps({"status": "ok", 'data': data})
                            self.main_logger.debug(f"{self.__class__}: Request: {request} response: {response}")
                            encrypted_text = self.encrypt_message(response)
                            rep_socket.send(encrypted_text)
                            continue
                        elif request['order'] == 'get_first':
                            if request['symbol'] in self.attached:
                                data = self.attached[request['symbol']][1].get_first()
                            else:
                                data = []

                            response = json.dumps({"status": "ok", 'data': data})
                            self.main_logger.debug(f"{self.__class__}: Request: {request} response: {response}")
                            encrypted_text = self.encrypt_message(response)
                            rep_socket.send(encrypted_text)
                            continue

                    response = json.dumps(
                        {"status": "warning", "message": f"Unknown action command: {request}"})
                    self.main_logger.warning(f"{self.__class__}: Request: {request} response: {response}")
                    encrypted_text = self.encrypt_message(response)
                    rep_socket.send(encrypted_text)

        except Exception as e:
            error_message = f"{self.__class__}: Error in Daemon's zmq_server: {e}"
            self.main_logger.error(error_message)
            response = json.dumps({"status": "error", "message": error_message})
            rep_socket.send_string(response)
        finally:
            rep_socket.close()
            context.destroy()
            self.main_logger.info(f"Zmq_server was terminated!")
            sys.exit(0)

    def collector(self):
        try:
            while not self.terminate:
                if self.status == 0:
                    self.seq_num = 1
                    self.connect()
                    time.sleep(5)
                elif self.status == 1:
                    time.sleep(1)

                else:
                    time.sleep(1)
        except Exception as e:
            self.main_logger.error(f"{self.__class__}: Error in collector: {e}")
        finally:
            self.main_logger.info(f"Collector main was terminated!")
            sys.exit(0)

    def connect(self):
        try:
            self.sock = socket.create_connection((self.host_fix, self.port_fix))
            self.sock.settimeout(60)
            self.main_logger.info("Connected to %s:%s", self.host_fix, self.port_fix)
            self.auth()
            if self.logged is True:
                self.main()
        except Exception as e:
            self.main_logger.error("Error during connection: %s", e)
        finally:
            try:
                self.sock.close()
            except Exception as e:
                pass
            else:
                self.main_logger.info("Disconnected from %s:%s", self.host_fix, self.port_fix)

    def auth(self):
        try:
            fix_message = self.login()
            self.main_logger.debug(f"Request: {fix_message.to_string()}")
            self.send_message(fix_message)
            self.main_logger.info("Sent login request.")

            response = self.sock.recv(1024)
            parser = simplefix.parser.FixParser()
            parser.append_buffer(response)
            response = parser.get_message()
            self.main_logger.debug(f"Response: {response.to_string()}")
            if response and response.get(35).decode() == 'A':
                self.logged = True
                self.main_logger.info("Logged in.")
            else:
                self.main_logger.info("Not logged  in.")
                self.logged = False
        except Exception as e:
            self.logged = False
            self.main_logger.error("Error during login: %s", e)

    def main(self):
        parser = simplefix.parser.FixParser()
        try:
            while True:
                if self.logged is False:
                    break
                cmd = self.cmd_queue.get_command()
                if cmd is not None:
                    # attach/detach
                    try:
                        if cmd['order'] == 'attach':
                            if cmd['symbol'] not in self.attached:
                                MDReqID = str(uuid.uuid4())
                                request = self.md_req(cmd['symbol'], subscription_request_type='1', MDReqID=MDReqID)
                                self.main_logger.debug(f"MD request: {request.to_string()}")
                                self.send_message(request)
                                self.attached[cmd['symbol']] = [MDReqID, FixData(10)]
                        elif cmd['order'] == 'detach':
                            if cmd['symbol'] in self.attached:
                                request = self.md_req(cmd['symbol'], subscription_request_type='2')
                                self.main_logger.debug(f"MD request: {request.to_string()}")
                                self.send_message(request)
                                del self.attached[cmd['symbol']]
                        else:
                            self.main_logger.warning(f"MD request order is invalid: {cmd['order']}")
                    except Exception as e:
                        self.main_logger.error("Error in main function, during message sent: %s", e)
                data = self.sock.recv(4096)
                if not data:
                    break
                parser.append_buffer(data)
                while True:
                    msg = parser.get_message()
                    if not msg:
                        break
                    self.timestamp_receive = time.time()
                    if msg.get(35).decode() == '0':
                        self.main_logger.debug(f"Received heartbeat: {msg.to_string()}")
                        self.send_heartbeat()
                    elif msg.get(35).decode() == '3':
                        self.main_logger.debug(f"Received REJECT: {msg.to_string()}")
                        self.logged = False
                        break
                    elif msg.get(35).decode() == 'W':
                        dict_md_req_id = self.transform_dict(self.attached)
                        if msg.get(262).decode() and msg.get(262).decode() in dict_md_req_id:
                            data = dict_md_req_id[msg.get(262).decode()]
                            self.add_prices(msg, data)
                            self.main_logger.debug(f"Received MD: {msg.to_string()}")
                    else:
                        self.main_logger.debug(f"Received other response: {msg.to_string()}")

                if time.time() > self.timestamp_sent + 15:
                    self.send_heartbeat()
                time.sleep(1)
        except Exception as e:
            self.main_logger.error("Error in main function, during message reception: %s", e)
        finally:
            self.logged = False

    def add_prices(self, msg, fix_data):
        prices = []
        for tag, value in msg.pairs:
            if tag == bytes(self.fix_tag_price, 'utf-8'):
                prices.append(float(value.decode('utf-8')))

        if len(prices) > 0:
            fix_data.add_data(prices[1], prices[0], int(time.time()))

    def transform_dict(self, x):
        result_dict = {}
        for key, value in x.items():
            if len(value) == 2:
                x1, y1 = value
                result_dict[x1] = y1
        return result_dict

    def send_heartbeat(self):
        try:
            fix_message = self.heartbeat()
            self.main_logger.debug(f"Sent heartbeat: {fix_message.to_string()}")
            self.send_message(fix_message)
        except Exception as e:
            self.logged = False
            self.main_logger.error("Error during heartbeat sending: %s", e)

    def send_message(self, msg):
        self.timestamp_sent = time.time()
        self.seq_num += 1
        self.sock.sendall(msg.encode())

    def fixtime(self):
        now = datetime.utcnow()
        return str(now.strftime('%Y%m%d-%H:%M:%S'))

    def login(self):
        # 8,9,35,49,56,34,52 must send
        msg = simplefix.FixMessage()
        msg.append_pair(8, self.fix_version)
        msg.append_pair(9, '111')
        msg.append_pair(35, 'A')
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        msg.append_pair(34, str(self.seq_num))
        msg.append_pair(52, self.fixtime())
        msg.append_pair(98, '0')
        msg.append_pair(554, self.password)
        msg.append_pair(141, 'Y')
        msg.append_pair(108, '30')
        return msg

    def heartbeat(self):
        # 8,9,35,49,56,34,52 must send
        msg = simplefix.FixMessage()
        msg.append_pair(8, self.fix_version)
        msg.append_pair(9, '111')
        msg.append_pair(35, '0')  # Heartbeat message type
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        msg.append_pair(34, str(self.seq_num))
        msg.append_pair(52, self.fixtime())
        return msg

    def md_req(self, symbol, subscription_request_type='1', MDReqID=str(uuid.uuid4())):
        # 8,9,35,49,56,34,52 must send
        msg = simplefix.FixMessage()
        msg.append_pair(8, self.fix_version)
        msg.append_pair(9, '164')
        msg.append_pair(35, 'V')  # V = Market Data Request
        msg.append_pair(49, self.sender_comp_id)
        msg.append_pair(56, self.target_comp_id)
        msg.append_pair(34, str(self.seq_num))
        msg.append_pair(52, self.fixtime())
        msg.append_pair(262, MDReqID)  # MDReqID
        msg.append_pair(263, subscription_request_type)
        msg.append_pair(264, '1')
        msg.append_pair(265, '0')
        msg.append_pair(267, '2')
        msg.append_pair(269, '0')
        msg.append_pair(269, '1')
        msg.append_pair(146, '1')
        msg.append_pair(55, symbol)
        msg.append_pair(48, symbol)
        msg.append_pair(22, '111')
        return msg


if __name__ == "__main__":
    fix = ExFixCollector()