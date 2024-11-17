import os
import socket
import threading
import struct
import time

# Global storage for keys, values, and expiry times
store = {}
expiry_times = {}
config = {"dir": "/tmp", "dbfilename": "dump.rdb"}  # Default configuration

def parse_redis_command(data):
    """Parse a RESP command into a list of strings."""
    lines = data.split(b"\r\n")
    command = []
    idx = 1  # Skip the first line (*n)

    while idx < len(lines) and lines[idx]:
        if lines[idx].startswith(b"$"):
            length = int(lines[idx][1:])  # Length of the next argument
            idx += 1  # Move to the actual argument
            command.append(lines[idx][:length].decode())
        idx += 1  # Move to the next length/command indicator

    return command

def read_size_encoded_value(data, index):
    """Decode size-encoded values."""
    first_byte = data[index]
    if first_byte >> 6 == 0:  # 00xxxxxx
        size = first_byte & 0x3F
        return size, index + 1
    elif first_byte >> 6 == 1:  # 01xxxxxx
        size = ((first_byte & 0x3F) << 8) | data[index + 1]
        return size, index + 2
    elif first_byte >> 6 == 2:  # 10xxxxxx
        size = struct.unpack(">I", data[index + 1:index + 5])[0]
        return size, index + 5
    else:
        raise ValueError("Unsupported size encoding")

def read_string(data, index):
    """Decode a string-encoded value."""
    size, index = read_size_encoded_value(data, index)
    string = data[index:index + size].decode()
    return string, index + size

def load_rdb_file():
    """Load key-value pairs from the RDB file."""
    global store
    filepath = os.path.join(config["dir"], config["dbfilename"])

    if not os.path.exists(filepath):
        print("RDB file not found, initializing empty database.")
        return

    with open(filepath, "rb") as file:
        data = file.read()

    index = 0
    # Verify header
    if data[:9] != b"REDIS0011":
        raise ValueError("Invalid RDB file header")
    index += 9

    # Skip metadata section
    while data[index] == 0xFA:
        _, index = read_string(data, index)  # Attribute name
        _, index = read_string(data, index)  # Attribute value

    # Parse database section
    while data[index] != 0xFF:  # End of file marker
        if data[index] == 0xFE:  # Database selector
            index += 2  # Skip database selector and index
        elif data[index] == 0xFB:  # Hash table size
            index += 2  # Skip hash table size information
        elif data[index] in (0xFC, 0xFD):  # Expire timestamps
            if data[index] == 0xFC:  # Milliseconds expiry
                index += 9
            else:  # Seconds expiry
                index += 5
        elif data[index] == 0x00:  # Key-value pair
            index += 1
            key, index = read_string(data, index)
            value, index = read_string(data, index)
            store[key] = value
        else:
            raise ValueError(f"Unexpected byte in RDB: {data[index]}")

def is_key_expired(key):
    """Check if a key is expired."""
    if key in expiry_times:
        if time.time() * 1000 >= expiry_times[key]:
            del store[key]
            del expiry_times[key]
            return True
    return False

def handle_echo_command(command):
    """Handle the ECHO command."""
    if len(command) != 2:
        return b"-ERR wrong number of arguments for 'ECHO' command\r\n"
    return f"${len(command[1])}\r\n{command[1]}\r\n".encode()

def handle_set_command(command):
    """Handle the SET command."""
    if len(command) < 3:
        return b"-ERR wrong number of arguments for 'SET' command\r\n"

    key = command[1]
    value = command[2]
    expiry = None

    # Handle PX option for expiry
    if len(command) > 3 and command[3].upper() == "PX" and len(command) > 4:
        try:
            expiry = int(command[4])
        except ValueError:
            return b"-ERR PX requires a valid integer\r\n"
        expiry_times[key] = time.time() * 1000 + expiry

    store[key] = value
    return b"+OK\r\n"

def handle_get_command(command):
    """Handle the GET command."""
    if len(command) != 2:
        return b"-ERR wrong number of arguments for 'GET' command\r\n"

    key = command[1]
    if is_key_expired(key):
        return b"$-1\r\n"
    if key in store:
        value = store[key]
        return f"${len(value)}\r\n{value}\r\n".encode()
    return b"$-1\r\n"

def handle_keys_command(command):
    """Handle the KEYS command."""
    if len(command) < 2 or command[1] != "*":
        return b"-ERR unsupported pattern\r\n"

    # Get all keys
    keys = [key for key in store if not is_key_expired(key)]
    response = f"*{len(keys)}\r\n"
    for key in keys:
        response += f"${len(key)}\r\n{key}\r\n"

    return response.encode()

def handle_client(client_socket):
    """Handles communication with a single client."""
    with client_socket:
        while True:
            data = client_socket.recv(1024)  # Read up to 1024 bytes
            if not data:
                break  # Client disconnected

            command = parse_redis_command(data)
            print(f"Received command: {command}")

            response = b"-ERR unknown command\r\n"

            # Route commands
            if command[0].upper() == "ECHO":
                response = handle_echo_command(command)
            elif command[0].upper() == "SET":
                response = handle_set_command(command)
            elif command[0].upper() == "GET":
                response = handle_get_command(command)
            elif command[0].upper() == "KEYS":
                response = handle_keys_command(command)
            client_socket.sendall(response)

def main():
    import sys

    # Parse arguments for RDB config
    args = sys.argv[1:]
    for i in range(len(args)):
        if args[i] == "--dir" and i + 1 < len(args):
            config["dir"] = args[i + 1]
        elif args[i] == "--dbfilename" and i + 1 < len(args):
            config["dbfilename"] = args[i + 1]

    print(f"Server is starting with configuration: {config}")
    print("Server listening for connections...")

    # Load the RDB file
    load_rdb_file()

    # Set up the server socket
    with socket.create_server(("localhost", 6379)) as server_socket:
        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()

if __name__ == "__main__":
    main()
