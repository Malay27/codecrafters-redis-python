import socket
import threading
import sys

# Store configuration parameters
config = {
    "dir": "/tmp",  # Default directory
    "dbfilename": "dump.rdb",  # Default filename
}

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

def handle_client(client_socket):
    """Handles communication with a single client."""
    with client_socket:
        while True:
            data = client_socket.recv(1024)  # Read up to 1024 bytes
            if not data:
                break  # Client disconnected

            command = parse_redis_command(data)
            print(f"Received command: {command}")

            # CONFIG GET command
            if command[0].upper() == "CONFIG" and len(command) >= 3 and command[1].upper() == "GET":
                param = command[2]
                if param in config:
                    key = param
                    value = config[param]
                    response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n".encode()
                else:
                    response = b"*0\r\n"  # Return an empty array if the parameter is unknown
            else:
                response = b"-ERR unknown command\r\n"

            client_socket.sendall(response)

def parse_arguments(args):
    """Parse command-line arguments for configuration parameters."""
    global config
    for i in range(1, len(args), 2):
        if args[i] == "--dir" and i + 1 < len(args):
            config["dir"] = args[i + 1]
        elif args[i] == "--dbfilename" and i + 1 < len(args):
            config["dbfilename"] = args[i + 1]

def main():
    # Parse command-line arguments
    parse_arguments(sys.argv)

    print(f"Server is starting with configuration: {config}")

    # Set up the server socket
    with socket.create_server(("localhost", 6379)) as server_socket:
        print("Server listening for connections...")

        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()

if __name__ == "__main__":
    main()
