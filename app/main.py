import socket  # noqa: F401

def main():
    print("Logs from your program will appear here!")

    # Set up the server socket
    with socket.create_server(("localhost", 6379)) as server_socket:
        # Continuously accept connections
        while True:
            client_socket, _ = server_socket.accept()
            print("Client connected, ready to respond to multiple PING commands...")

            # Keep the client socket open to allow multiple commands
            try:
                with client_socket:
                    while True:
                        # Read data from the client
                        data = client_socket.recv(1024)  # Read up to 1024 bytes
                        if not data:
                            break  # Client disconnected

                        print("Received command, sending +PONG response...")

                        # Send the hardcoded +PONG\r\n response for each command
                        client_socket.sendall(b"+PONG\r\n")

            except ConnectionResetError:
                print("Connection closed by client.")

if __name__ == "__main__":
    main()
