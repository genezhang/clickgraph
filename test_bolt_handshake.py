"""
Simple Bolt handshake test to debug connection issues.
"""
import socket
import struct

HOST = 'localhost'
PORT = 7687

# Bolt magic preamble
BOLT_MAGIC = bytes([0x60, 0x60, 0xB0, 0x17])

# Client versions (4 versions, 4 bytes each)
# Request Bolt 4.4, 4.3, 4.2, 4.1
CLIENT_VERSIONS = struct.pack('>IIII', 0x00000404, 0x00000403, 0x00000402, 0x00000401)

print(f"Connecting to {HOST}:{PORT}...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    sock.connect((HOST, PORT))
    print("✅ Connected!")
    
    # Send handshake
    print(f"Sending Bolt magic: {BOLT_MAGIC.hex()}")
    sock.sendall(BOLT_MAGIC)
    
    print(f"Sending client versions: {CLIENT_VERSIONS.hex()}")
    sock.sendall(CLIENT_VERSIONS)
    
    # Receive negotiated version (4 bytes)
    print("Waiting for server response...")
    response = sock.recv(4)
    
    if len(response) == 4:
        version = struct.unpack('>I', response)[0]
        print(f"✅ Server negotiated version: 0x{version:08x}")
        
        if version == 0x00000404:
            print("✅ Bolt 4.4 negotiated successfully!")
        elif version == 0:
            print("❌ Server rejected all versions (returned 0x00000000)")
        else:
            print(f"✅ Server negotiated version: {version >> 16}.{version & 0xFFFF}")
    else:
        print(f"❌ Incomplete response: got {len(response)} bytes, expected 4")
        print(f"Response: {response.hex()}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    sock.close()
    print("Connection closed")
