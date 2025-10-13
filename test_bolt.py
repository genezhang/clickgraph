#!/usr/bin/env python3
"""
Test script for ClickGraph Bolt protocol compatibility.
This script tests Neo4j driver connectivity to our Bolt server.
"""

import socket
import struct
import time

def test_bolt_handshake():
    """Test Bolt protocol handshake with ClickGraph server."""
    try:
        # Connect to ClickGraph Bolt server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)  # 10 second timeout
        
        print("Connecting to ClickGraph Bolt server on localhost:7687...")
        sock.connect(('localhost', 7687))
        print("✅ Successfully connected!")
        
        # Send Bolt handshake (magic bytes + supported versions)
        # Magic: 0x6060B017, Versions: [4, 3, 0, 0]
        handshake = struct.pack('>I4I', 0x6060B017, 4, 3, 0, 0)
        print(f"Sending handshake: {handshake.hex()}")
        
        sock.send(handshake)
        
        # Read server response (4 bytes for selected version)
        response = sock.recv(4)
        if len(response) == 4:
            version = struct.unpack('>I', response)[0]
            print(f"✅ Server selected Bolt version: {version}")
            
            if version in [3, 4]:
                print("🎉 Bolt handshake successful!")
                return True
            else:
                print(f"❌ Unsupported version returned: {version}")
                return False
        else:
            print(f"❌ Invalid response length: {len(response)} bytes")
            return False
            
    except socket.timeout:
        print("❌ Connection timeout - server may not be responding")
        return False
    except ConnectionRefusedError:
        print("❌ Connection refused - server may not be running")
        return False
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False
    finally:
        try:
            sock.close()
        except:
            pass

def test_cypher_via_bolt():
    """Test sending a simple Cypher query via Bolt protocol."""
    try:
        print("\n" + "="*50)
        print("Testing Cypher query execution via Bolt protocol")
        print("="*50)
        
        # For now, we'll just test the handshake since implementing
        # the full Bolt message protocol is complex
        success = test_bolt_handshake()
        
        if success:
            print("\n✅ Bolt protocol handshake successful!")
            print("📝 Next: Implement full message protocol for query execution")
            return True
        else:
            print("\n❌ Bolt protocol handshake failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Cypher via Bolt: {e}")
        return False

if __name__ == "__main__":
    print("ClickGraph Bolt Protocol Test")
    print("=" * 40)
    
    # Wait a moment for server to be ready
    time.sleep(1)
    
    success = test_cypher_via_bolt()
    
    if success:
        print("\n🎉 Bolt protocol test completed successfully!")
        print("✅ ClickGraph server accepts Bolt connections")
    else:
        print("\n❌ Bolt protocol test failed")
        print("🔧 Check server logs and connection settings")
    
    exit(0 if success else 1)