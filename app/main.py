import socket
import threading

# Dictionary to store key-value pairs
data_store = {}

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
        print("Client connected, ready to respond to multiple commands...")
        
        while True:
            data = client_socket.recv(1024)  # Read up to 1024 bytes
            if not data:
                break  # Client disconnected

            command = parse_redis_command(data)
            print(f"Received command: {command}")

            # PING command
            if command[0].upper() == "PING":
                response = b"+PONG\r\n"
            
            # ECHO command
            elif command[0].upper() == "ECHO" and len(command) > 1:
                message = command[1]
                response = f"${len(message)}\r\n{message}\r\n".encode()
            
            # SET command
            elif command[0].upper() == "SET" and len(command) > 2:
                key, value = command[1], command[2]
                data_store[key] = value  # Store the key-value pair
                response = b"+OK\r\n"  # RESP response for successful SET
            
            # GET command
            elif command[0].upper() == "GET" and len(command) > 1:
                key = command[1]
                if key in data_store:
                    value = data_store[key]
                    response = f"${len(value)}\r\n{value}\r\n".encode()
                else:
                    response = b"$-1\r\n"  # Null bulk reply if key not found
            
            # Unknown command
            else:
                response = b"-ERR unknown command\r\n"
            
            client_socket.sendall(response)

def main():
    print("Server is starting on localhost:6379...")

    with socket.create_server(("localhost", 6379)) as server_socket:
        print("Server listening for connections...")

        while True:
            client_socket, _ = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()

if __name__ == "__main__":
    main()
