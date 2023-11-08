import os
from cryptography.fernet import Fernet

if not os.path.exists("fernet.key"):
    key = Fernet.generate_key()
    with open("fernet.key", "wb") as key_file:
        key_file.write(key)

