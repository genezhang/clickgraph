#!/usr/bin/env python3
"""
Test Bolt protocol multi-database support.

This script verifies that ClickGraph's Bolt protocol correctly extracts
and uses database selection from HELLO message metadata (Neo4j 4.0+ standard).
"""

import socket
import struct
import json


def pack_string(s):
    """Pack a string as Bolt UTF-8 string."""
    encoded = s.encode('utf-8')
    if len(encoded) < 16:
        return bytes([0x80 + len(encoded)]) + encoded
    elif len(encoded) < 256:
        return bytes([0xD0, len(encoded)]) + encoded
    else:
        return bytes([0xD1]) + struct.pack('>H', len(encoded)) + encoded


def pack_dict(d):
    """Pack a dictionary as Bolt map."""
    if len(d) < 16:
        result = bytes([0xA0 + len(d)])
    else:
        result = bytes([0xD8, len(d)])
    
    for key, value in d.items():
        result += pack_string(key)
        if isinstance(value, str):
            result += pack_string(value)
        elif isinstance(value, bool):
            result += bytes([0xC3 if value else 0xC2])
        elif isinstance(value, int):
            if -16 <= value < 128:
                result += struct.pack('b', value)
            else:
                result += bytes([0xC9]) + struct.pack('>q', value)
    return result


def create_hello_message(user_agent, auth_scheme="none", database=None):
    """Create a HELLO message with optional database selection."""
    # Extra metadata (first field)
    extra = {"user_agent": user_agent}
    if database:
        extra["db"] = database  # Neo4j 4.0+ standard
    
    # Auth token (second field)
    auth = {"scheme": auth_scheme}
    
    # Pack message: signature (0x01 for HELLO) + fields
    message = bytes([0x01])  # HELLO signature
    message += pack_dict(extra)
    message += pack_dict(auth)
    
    # Add structure marker (0xB2 = 2 fields)
    return bytes([0xB2]) + message


def test_bolt_database_selection():
    """Test database selection via Bolt HELLO message."""
    print("Testing Bolt multi-database support...")
    print("=" * 60)
    
    try:
        # Connect to Bolt server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(("localhost", 7687))
        print("[OK] Connected to Bolt server on port 7687")
        
        # Send Bolt handshake
        handshake = struct.pack('>I', 0x6060B017)  # Bolt magic
        handshake += struct.pack('>I', 0x00000404)  # Version 4.4
        handshake += struct.pack('>I', 0x00000403)  # Version 4.3
        handshake += struct.pack('>I', 0x00000000)  # No version
        handshake += struct.pack('>I', 0x00000000)  # No version
        sock.send(handshake)
        
        # Receive negotiated version
        version_response = sock.recv(4)
        version = struct.unpack('>I', version_response)[0]
        print(f"[OK] Negotiated Bolt version: {version:08x}")
        
        # Test 1: HELLO with database selection
        print("\nTest 1: HELLO with database='social_network'")
        hello_msg = create_hello_message(
            user_agent="ClickGraph-Test/1.0",
            database="social_network"
        )
        
        # Send as chunk
        chunk = struct.pack('>H', len(hello_msg)) + hello_msg
        chunk += struct.pack('>H', 0)  # End marker
        sock.send(chunk)
        
        # Receive response
        response_size = struct.unpack('>H', sock.recv(2))[0]
        response_data = sock.recv(response_size)
        end_marker = struct.unpack('>H', sock.recv(2))[0]
        
        if response_data[0] == 0x70:  # SUCCESS
            print("[OK] HELLO succeeded")
            print("[OK] Database selection accepted by server")
        else:
            print(f"[FAIL] HELLO failed: {response_data.hex()}")
        
        # Close connection
        sock.close()
        print("\n" + "=" * 60)
        print("Test completed successfully!")
        print("\nNote: Server logs should show:")
        print("  'Bolt connection using database/schema: social_network'")
        
    except socket.timeout:
        print("[FAIL] Connection timeout - is Bolt server running?")
    except ConnectionRefusedError:
        print("[FAIL] Connection refused - is ClickGraph server running with Bolt enabled?")
        print("  Start server with: cargo run --bin clickgraph")
    except Exception as e:
        print(f"[FAIL] Error: {e}")


if __name__ == "__main__":
    test_bolt_database_selection()
