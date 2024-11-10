import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    # Set up the server socket 
    with socket.create_server(("localhost", 6379)) as server_socket:
        # Continuously accept connections
        while True:
            client_socket,_=server_socket.accept()
            with client_socket:
                print("Client connected, sending +PONG response...")

                client_socket.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
