"""
Test Bolt HELLO message after handshake.
"""
import socket
import struct

HOST = 'localhost'
PORT = 7687

# Bolt magic preamble
BOLT_MAGIC = bytes([0x60, 0x60, 0xB0, 0x17])

# Client versions
CLIENT_VERSIONS = struct.pack('>IIII', 0x00000404, 0x00000403, 0x00000402, 0x00000401)

def pack_string(s):
    """Pack a string as Bolt string (tiny/8/16/32)"""
    encoded = s.encode('utf-8')
    length = len(encoded)
    if length < 16:
        # Tiny string
        return bytes([0x80 + length]) + encoded
    elif length < 256:
        # String 8
        return bytes([0xD0, length]) + encoded
    else:
        # String 16
        return bytes([0xD1]) + struct.pack('>H', length) + encoded

def pack_map(m):
    """Pack a dictionary as Bolt map"""
    size = len(m)
    if size < 16:
        header = bytes([0xA0 + size])
    else:
        header = bytes([0xD8, size])
    
    result = header
    for key, value in m.items():
        result += pack_string(key)
        result += pack_string(value)
    return result

def send_chunked(sock, data):
    """Send data as chunked Bolt message"""
    # Send the data as a single chunk
    sock.sendall(struct.pack('>H', len(data)) + data)
    # Send end marker (0x0000)
    sock.sendall(struct.pack('>H', 0))

def read_response(sock):
    """Read a chunked Bolt message"""
    chunks = []
    while True:
        size_bytes = sock.recv(2)
        if len(size_bytes) != 2:
            print(f"❌ Failed to read chunk size (got {len(size_bytes)} bytes)")
            return None
        
        chunk_size = struct.unpack('>H', size_bytes)[0]
        if chunk_size == 0:
            break
        
        chunk_data = sock.recv(chunk_size)
        if len(chunk_data) != chunk_size:
            print(f"❌ Failed to read chunk (expected {chunk_size}, got {len(chunk_data)})")
            return None
        
        chunks.append(chunk_data)
    
    return b''.join(chunks)

print(f"Connecting to {HOST}:{PORT}...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5.0)  # 5 second timeout

try:
    sock.connect((HOST, PORT))
    print("✅ Connected!")
    
    # Send handshake
    print(f"Sending Bolt handshake...")
    sock.sendall(BOLT_MAGIC + CLIENT_VERSIONS)
    
    # Receive negotiated version
    response = sock.recv(4)
    if len(response) == 4:
        version = struct.unpack('>I', response)[0]
        print(f"✅ Negotiated Bolt 4.{version & 0xFF}")
    else:
        print(f"❌ Handshake failed")
        exit(1)
    
    # Send HELLO message
    print("\nSending HELLO message...")
    hello_data = {
        "user_agent": "ClickGraph Test/1.0",
        "scheme": "basic",
        "principal": "neo4j",
        "credentials": "password"
    }
    
    # Message structure: 0xB1 (TinyStruct size 1) + 0x01 (HELLO tag) + map
    message = bytes([0xB1, 0x01]) + pack_map(hello_data)
    print(f"HELLO message: {message.hex()}")
    
    send_chunked(sock, message)
    print("✅ HELLO sent, waiting for response...")
    
    # Read SUCCESS response
    response_data = read_response(sock)
    if response_data:
        print(f"✅ Received response: {len(response_data)} bytes")
        print(f"Response data: {response_data.hex()}")
        
        # Parse response structure
        if len(response_data) >= 2:
            struct_marker = response_data[0]
            tag = response_data[1]
            
            if struct_marker == 0xB1 and tag == 0x70:
                print("✅ Received SUCCESS message!")
            elif struct_marker == 0xB1 and tag == 0x7F:
                print("❌ Received FAILURE message")
            else:
                print(f"⚠️  Unknown message: struct=0x{struct_marker:02x}, tag=0x{tag:02x}")
    else:
        print("❌ No response received (timeout or connection closed)")
        
except socket.timeout:
    print("❌ Timeout waiting for response")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    sock.close()
    print("\nConnection closed")
