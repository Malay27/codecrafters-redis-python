import socket  # noqa: F401
import threading


def handle_client(client_socket):
    print("Handles communication with a single client.")
    with client_socket:
        print("Client connected, ready to respond to multiple PING commands...")
        while True:
            # Read data from the client
            data = client_socket.recv(1024)  # Read up to 1024 bytes
            if not data:
                break  # Client disconnected

            print("Received command, sending +PONG response...")

            # Send the hardcoded +PONG\r\n response for each command
            client_socket.sendall(b"+PONG\r\n")

def main():
    print("Logs from your program will appear here!")

    # Set up the server socket
    with socket.create_server(("localhost", 6379)) as server_socket:
        # Continuously accept connections
        while True:
            client_socket, _ = server_socket.accept()
            print("Client connected, ready to respond to multiple PING commands...")

            # Create a new thread for each client
            client_thread=threading.Thread(target=handle_client,args=(client_socket,))
            client_thread.start()

if __name__ == "__main__":
    main()
