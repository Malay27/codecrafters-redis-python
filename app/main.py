import socket
import threading
import time

# Store key-value pairs and expiry times
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

def handle_ping_command():
    """Handle the PING command."""
    return b"+PONG\r\n"

def handle_set_command(command):
    """Handle the SET command with optional PX expiry."""
    global store, expiry_times

    if len(command) < 3:
        return b"-ERR wrong number of arguments for 'SET' command\r\n"

    key = command[1]
    value = command[2]
    expiry = None

    # Check for PX parameter
    if len(command) > 4 and command[3].lower() == "px":
        try:
            expiry = int(command[4])
        except ValueError:
            return b"-ERR PX value is not an integer\r\n"

    # Store the key-value pair
    store[key] = value

    # Set expiry if PX is provided
    if expiry is not None:
        expiry_times[key] = time.time() + (expiry / 1000)  # Convert ms to seconds

    return b"+OK\r\n"

def handle_get_command(command):
    """Handle the GET command."""
    global store

    if len(command) < 2:
        return b"-ERR wrong number of arguments for 'GET' command\r\n"

    key = command[1]

    # Check for expired keys
    check_and_remove_expired_keys()

    value = store.get(key)
    if value is not None:
        return f"${len(value)}\r\n{value}\r\n".encode()
    else:
        return b"$-1\r\n"  # Null bulk string for missing keys

def handle_echo_command(command):
    """Handle the ECHO command."""
    if len(command) < 2:
        return b"-ERR wrong number of arguments for 'ECHO' command\r\n"

    message = command[1]
    return f"${len(message)}\r\n{message}\r\n".encode()

def handle_config_get_command(command):
    """Handle the CONFIG GET command."""
    if len(command) < 3:
        return b"-ERR wrong number of arguments for 'CONFIG GET' command\r\n"

    param = command[2].lower()

    if param in config:
        value = config[param]
        return f"*2\r\n${len(param)}\r\n{param}\r\n${len(value)}\r\n{value}\r\n".encode()
    else:
        return b"$-1\r\n"  # Null bulk string for unknown parameter

def check_and_remove_expired_keys():
    """Remove keys that have expired."""
    global expiry_times, store
    current_time = time.time()
    expired_keys = [key for key, expiry in expiry_times.items() if expiry <= current_time]
    for key in expired_keys:
        expiry_times.pop(key, None)
        store.pop(key, None)

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

            # Process commands
            if command[0].upper() == "PING":
                response = handle_ping_command()
            elif command[0].upper() == "SET":
                response = handle_set_command(command)
            elif command[0].upper() == "GET":
                response = handle_get_command(command)
            elif command[0].upper() == "ECHO":
                response = handle_echo_command(command)
            elif command[0].upper() == "CONFIG" and len(command) > 1 and command[1].upper() == "GET":
                response = handle_config_get_command(command)

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

    # Set up the server socket
    with socket.create_server(("localhost", 6379)) as server_socket:
        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()

if __name__ == "__main__":
    main()
