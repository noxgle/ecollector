# Exante Data Collector for FIX Protocol

**Warning**: This code is provided as-is, and its usage is at your own risk. It's highly recommended to review and thoroughly test the code to ensure it meets your specific requirements, as there's no guarantee of its functionality or suitability for your use case.

This script is framework for collecting market data from Exante using the FIX protocol, with features for
attaching and detaching symbols, managing market data, and handling authentication and encryption. It uses
multithreading to manage data collection and communication with clients via ZeroMQ.',



**Requirements**:

To use this script, you'll need to install the following Python packages:

- [cryptography](https://pypi.org/project/cryptography): Used for encryption.
- [pyzmq](https://pypi.org/project/pyzmq): Required for ZeroMQ communication.
- [simplefix](https://pypi.org/project/simplefix): Used for parsing FIX messages.

pip install cryptography pyzmq simplefix

**How to download**

You can clone all or download only collector:

Web browser: [Exante FIX collector](https://lvk.pl/9jQf2Eg)

Console: wget -O ExFixCollector.py https://lvk.pl/9jQf2Eg

**How to Test**:

1. Create a Fernet key by running the `create_fernet_key.py` script.
2. Edit and customize the `fix_demo_conf.ini` configuration file to match your requirements.
3. Run the example by executing the `ecollector_test.py` script.

[Shorten link](https://lvk.pl)