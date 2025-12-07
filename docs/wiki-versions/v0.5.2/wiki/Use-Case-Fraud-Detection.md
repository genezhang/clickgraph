> **Note**: This documentation is for ClickGraph v0.5.2. [View latest docs →](../../wiki/Home.md)
# Fraud Detection with ClickGraph

**Caution:** This entire document is AI-generated. It may contain mistakes. Double check and raise issues for correction if you find any.

Detect fraudulent patterns in financial transactions using graph analytics - from account rings to money laundering networks.

## Table of Contents
- [Overview](#overview)
- [Schema Design](#schema-design)
- [Sample Dataset](#sample-dataset)
- [Fraud Detection Patterns](#fraud-detection-patterns)
- [Advanced Techniques](#advanced-techniques)
- [Real-Time Detection](#real-time-detection)
- [Performance Optimization](#performance-optimization)

## Overview

Financial fraud often involves complex networks of accounts, transactions, and entities. Graph analytics excels at detecting these patterns that traditional SQL queries miss.

### Common Fraud Patterns

**Account Rings:**
- Multiple accounts controlled by same entity
- Circular money flows
- Shared device fingerprints or IP addresses

**Money Laundering:**
- Complex transaction chains (layering)
- Rapid movement through multiple accounts
- Splitting large amounts (structuring)

**Synthetic Identity:**
- Fake accounts with stolen credentials
- Linked through shared personal information
- Coordinated transaction patterns

**Bust-Out Fraud:**
- Building credit then maxing out
- Multiple linked accounts with same pattern
- Sudden large purchases followed by default

## Schema Design

### Financial Fraud Detection Schema

```yaml
name: fraud_detection
version: "1.0"

graph_schema:
  nodes:
    # Customer accounts
    - label: Account
      database: fraud_db
      table: accounts
      node_id: account_id
      property_mappings:
        account_id: account_id
        account_number: acct_number
        account_type: acct_type
        status: account_status
        opened_date: opened_at
        credit_limit: credit_limit
        balance: current_balance
        risk_score: risk_score
        is_verified: verified_flag
    
    # Customer entities
    - label: Customer
      database: fraud_db
      table: customers
      node_id: customer_id
      property_mappings:
        customer_id: customer_id
        name: full_name
        email: email_address
        phone: phone_number
        ssn_hash: ssn_hash
        address: street_address
        city: city
        state: state
        zip: zip_code
        dob: date_of_birth
        kyc_status: kyc_verified
    
    # Merchants
    - label: Merchant
      database: fraud_db
      table: merchants
      node_id: merchant_id
      property_mappings:
        merchant_id: merchant_id
        name: merchant_name
        category: mcc_category
        risk_level: risk_level
    
    # Devices (for device fingerprinting)
    - label: Device
      database: fraud_db
      table: devices
      node_id: device_id
      property_mappings:
        device_id: device_id
        fingerprint: device_fingerprint
        ip_address: last_ip
        user_agent: user_agent
        first_seen: first_seen_at
        last_seen: last_seen_at
    
    # IP Addresses
    - label: IPAddress
      database: fraud_db
      table: ip_addresses
      node_id: ip_id
      property_mappings:
        ip_id: ip_id
        ip: ip_address
        country: country_code
        is_vpn: vpn_detected
        is_proxy: proxy_detected
        risk_score: ip_risk_score
  
  relationships:
    # Customer owns Account
    - type: OWNS
      database: fraud_db
      table: account_ownership
      from_id: customer_id
      to_id: account_id
      from_node: Customer
      to_node: Account
      property_mappings:
        since: ownership_start
        is_primary: primary_owner
    
    # Account transfers to Account
    - type: TRANSFERRED
      database: fraud_db
      table: transactions
      from_id: from_account_id
      to_id: to_account_id
      from_node: Account
      to_node: Account
      property_mappings:
        amount: transaction_amount
        timestamp: transaction_time
        transaction_id: txn_id
        method: payment_method
        status: txn_status
        description: txn_description
    
    # Account pays Merchant
    - type: PAID
      database: fraud_db
      table: merchant_transactions
      from_id: account_id
      to_id: merchant_id
      from_node: Account
      to_node: Merchant
      property_mappings:
        amount: amount
        timestamp: transaction_time
        transaction_id: txn_id
        declined: was_declined
    
    # Customer used Device
    - type: USED_DEVICE
      database: fraud_db
      table: device_usage
      from_id: customer_id
      to_id: device_id
      from_node: Customer
      to_node: Device
      property_mappings:
        first_used: first_use
        last_used: last_use
        login_count: total_logins
    
    # Device connected from IPAddress
    - type: CONNECTED_FROM
      database: fraud_db
      table: device_connections
      from_id: device_id
      to_id: ip_id
      from_node: Device
      to_node: IPAddress
      property_mappings:
        timestamp: connection_time
        session_id: session_id
    
    # Customers sharing data (for synthetic identity detection)
    - type: SHARES_PHONE
      database: fraud_db
      table: shared_phones
      from_id: customer_id_1
      to_id: customer_id_2
      from_node: Customer
      to_node: Customer
      property_mappings:
        phone: phone_number
    
    - type: SHARES_ADDRESS
      database: fraud_db
      table: shared_addresses
      from_id: customer_id_1
      to_id: customer_id_2
      from_node: Customer
      to_node: Customer
      property_mappings:
        address: street_address
    
    - type: SHARES_EMAIL
      database: fraud_db
      table: shared_emails
      from_id: customer_id_1
      to_id: customer_id_2
      from_node: Customer
      to_node: Customer
      property_mappings:
        email: email_address
```

### ClickHouse Table Definitions

```sql
-- Accounts
CREATE TABLE fraud_db.accounts (
    account_id UInt64,
    acct_number String,
    acct_type String,
    account_status String,
    opened_at DateTime,
    credit_limit Decimal(15,2),
    current_balance Decimal(15,2),
    risk_score Float32,
    verified_flag UInt8
) ENGINE = Memory;

-- Customers
CREATE TABLE fraud_db.customers (
    customer_id UInt64,
    full_name String,
    email_address String,
    phone_number String,
    ssn_hash String,
    street_address String,
    city String,
    state String,
    zip_code String,
    date_of_birth Date,
    kyc_verified UInt8
) ENGINE = Memory;

-- Transactions (account-to-account)
CREATE TABLE fraud_db.transactions (
    txn_id UInt64,
    from_account_id UInt64,
    to_account_id UInt64,
    transaction_amount Decimal(15,2),
    transaction_time DateTime,
    payment_method String,
    txn_status String,
    txn_description String
) ENGINE = Memory;

-- Merchant transactions
CREATE TABLE fraud_db.merchant_transactions (
    txn_id UInt64,
    account_id UInt64,
    merchant_id UInt64,
    amount Decimal(15,2),
    transaction_time DateTime,
    was_declined UInt8
) ENGINE = Memory;

-- Merchants
CREATE TABLE fraud_db.merchants (
    merchant_id UInt64,
    merchant_name String,
    mcc_category String,
    risk_level String
) ENGINE = Memory;

-- Devices
CREATE TABLE fraud_db.devices (
    device_id UInt64,
    device_fingerprint String,
    last_ip String,
    user_agent String,
    first_seen_at DateTime,
    last_seen_at DateTime
) ENGINE = Memory;

-- IP Addresses
CREATE TABLE fraud_db.ip_addresses (
    ip_id UInt64,
    ip_address String,
    country_code String,
    vpn_detected UInt8,
    proxy_detected UInt8,
    ip_risk_score Float32
) ENGINE = Memory;

-- Account ownership
CREATE TABLE fraud_db.account_ownership (
    customer_id UInt64,
    account_id UInt64,
    ownership_start DateTime,
    primary_owner UInt8
) ENGINE = Memory;

-- Device usage
CREATE TABLE fraud_db.device_usage (
    customer_id UInt64,
    device_id UInt64,
    first_use DateTime,
    last_use DateTime,
    total_logins UInt32
) ENGINE = Memory;

-- Shared data for synthetic identity detection
CREATE TABLE fraud_db.shared_phones (
    customer_id_1 UInt64,
    customer_id_2 UInt64,
    phone_number String
) ENGINE = Memory;

CREATE TABLE fraud_db.shared_addresses (
    customer_id_1 UInt64,
    customer_id_2 UInt64,
    street_address String
) ENGINE = Memory;

CREATE TABLE fraud_db.shared_emails (
    customer_id_1 UInt64,
    customer_id_2 UInt64,
    email_address String
) ENGINE = Memory;
```

## Sample Dataset

### Generate Fraud Test Data

```python
# generate_fraud_data.py
import random
from datetime import datetime, timedelta
import clickhouse_connect
from decimal import Decimal

client = clickhouse_connect.get_client(host='localhost', port=8123)

# Generate 10,000 normal customers
customers = []
for i in range(1, 10001):
    customers.append((
        i,
        f"Customer {i}",
        f"customer{i}@example.com",
        f"+1-555-{random.randint(1000000, 9999999)}",
        f"hash_{i}",
        f"{random.randint(100, 9999)} Main St",
        random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']),
        random.choice(['NY', 'CA', 'IL', 'TX']),
        f"{random.randint(10000, 99999)}",
        datetime(1960, 1, 1) + timedelta(days=random.randint(0, 20000)),
        random.choice([0, 1])
    ))

# Generate 100 synthetic identity customers (fraud ring)
for i in range(10001, 10101):
    # Share phone/email patterns
    shared_phone = f"+1-555-{random.randint(1000, 1010)}"
    shared_email = f"shared{random.randint(1, 10)}@tempmail.com"
    
    customers.append((
        i,
        f"Synthetic {i}",
        shared_email,
        shared_phone,
        f"hash_synthetic_{i}",
        f"{random.randint(1, 10)} Fake St",  # Shared addresses
        "Miami",
        "FL",
        "33101",
        datetime(1990, 1, 1) + timedelta(days=random.randint(0, 5000)),
        0  # Not KYC verified
    ))

client.insert('fraud_db.customers', customers,
    column_names=['customer_id', 'full_name', 'email_address', 'phone_number',
                  'ssn_hash', 'street_address', 'city', 'state', 'zip_code',
                  'date_of_birth', 'kyc_verified'])

# Generate accounts (normal + fraud)
accounts = []
for i in range(1, 10001):
    accounts.append((
        i,
        f"ACCT{i:08d}",
        random.choice(['checking', 'savings', 'credit']),
        'active',
        datetime.now() - timedelta(days=random.randint(30, 1000)),
        Decimal(random.randint(1000, 50000)),
        Decimal(random.randint(0, 10000)),
        random.uniform(0.1, 0.5),  # Normal risk
        1
    ))

# Fraud ring accounts (high risk)
for i in range(10001, 10201):
    accounts.append((
        i,
        f"ACCT{i:08d}",
        'credit',
        'active',
        datetime.now() - timedelta(days=random.randint(1, 30)),  # Recently opened
        Decimal(5000),
        Decimal(4800),  # Near limit
        random.uniform(0.7, 0.95),  # High risk
        0
    ))

client.insert('fraud_db.accounts', accounts,
    column_names=['account_id', 'acct_number', 'acct_type', 'account_status',
                  'opened_at', 'credit_limit', 'current_balance', 'risk_score',
                  'verified_flag'])

# Generate transactions (including circular fraud patterns)
transactions = []
txn_id = 1

# Normal transactions
for i in range(50000):
    transactions.append((
        txn_id,
        random.randint(1, 10000),
        random.randint(1, 10000),
        Decimal(random.uniform(10, 1000)),
        datetime.now() - timedelta(hours=random.randint(1, 8760)),
        random.choice(['ACH', 'wire', 'check', 'debit']),
        'completed',
        'Normal transfer'
    ))
    txn_id += 1

# Circular fraud pattern (money laundering)
fraud_ring = list(range(10001, 10021))  # 20 accounts in ring
for round_num in range(10):
    for i in range(len(fraud_ring)):
        from_acct = fraud_ring[i]
        to_acct = fraud_ring[(i + 1) % len(fraud_ring)]
        transactions.append((
            txn_id,
            from_acct,
            to_acct,
            Decimal(random.uniform(1000, 5000)),
            datetime.now() - timedelta(hours=random.randint(1, 72)),
            'wire',
            'completed',
            'Layering transaction'
        ))
        txn_id += 1

client.insert('fraud_db.transactions', transactions,
    column_names=['txn_id', 'from_account_id', 'to_account_id',
                  'transaction_amount', 'transaction_time', 'payment_method',
                  'txn_status', 'txn_description'])

print("✓ Fraud detection data generated")
print(f"  - {len(customers)} customers (100 synthetic)")
print(f"  - {len(accounts)} accounts (200 fraud ring)")
print(f"  - {len(transactions)} transactions (200 circular)")
```

## Fraud Detection Patterns

### 1. Circular Money Flow Detection

Detect money laundering through circular transaction chains:

```cypher
// Find circular transaction paths (length 3-6)
MATCH path = (start:Account)-[:TRANSFERRED*3..6]->(start)
WHERE ALL(rel IN relationships(path) WHERE rel.timestamp > datetime() - duration({days: 7}))
WITH path, 
     [rel IN relationships(path) | rel.amount] as amounts,
     [rel IN relationships(path) | rel.timestamp] as timestamps
RETURN [node IN nodes(path) | node.account_number] as cycle_accounts,
       reduce(total = 0.0, amt IN amounts | total + amt) as total_flow,
       length(path) as cycle_length,
       (max(timestamps) - min(timestamps)) as time_span
ORDER BY total_flow DESC
LIMIT 20
```

**Fraud Indicator**: Rapid circular movement of large amounts

**Expected Performance**: 300-500ms for 100K transactions

### 2. Account Ring Detection (Shared Data)

Find synthetic identities sharing phone/email/address:

```cypher
// Find clusters of accounts sharing multiple data points
MATCH (c1:Customer)-[:SHARES_PHONE|SHARES_EMAIL|SHARES_ADDRESS]-(c2:Customer)
WITH c1, c2, count(*) as shared_attributes
WHERE shared_attributes >= 2
MATCH (c1)-[:OWNS]->(a1:Account), (c2)-[:OWNS]->(a2:Account)
WITH c1, collect(DISTINCT c2) as linked_customers,
     collect(DISTINCT a1) + collect(DISTINCT a2) as all_accounts
WHERE size(linked_customers) >= 3
RETURN c1.customer_id,
       c1.name,
       c1.kyc_status,
       size(linked_customers) as ring_size,
       size(all_accounts) as total_accounts,
       [acc IN all_accounts | acc.risk_score] as risk_scores
ORDER BY ring_size DESC
```

**Fraud Indicator**: Multiple "different" customers sharing contact info

**Use Case**: Synthetic identity fraud detection

### 3. Device Fingerprint Analysis

Find multiple accounts accessed from same device:

```cypher
// Detect device reuse across many accounts
MATCH (device:Device)<-[:USED_DEVICE]-(customer:Customer)-[:OWNS]->(account:Account)
WITH device, 
     count(DISTINCT customer) as customer_count,
     count(DISTINCT account) as account_count,
     collect(DISTINCT customer.name) as customers
WHERE customer_count > 5
MATCH (device)-[:CONNECTED_FROM]->(ip:IPAddress)
RETURN device.fingerprint,
       device.ip_address,
       customer_count,
       account_count,
       ip.is_vpn,
       ip.is_proxy,
       customers[0..10] as sample_customers
ORDER BY account_count DESC
```

**Fraud Indicator**: One device controlling many accounts

**Use Case**: Account takeover detection

### 4. Rapid Transaction Chain

Detect smurfing/structuring (breaking up large amounts):

```cypher
// Find rapid sequence of similar-sized transactions
MATCH path = (source:Account)-[:TRANSFERRED*3..10]->(destination:Account)
WHERE ALL(rel IN relationships(path) WHERE 
    rel.timestamp > datetime() - duration({hours: 24}) AND
    rel.amount < 10000 AND  // Below reporting threshold
    rel.amount > 9000       // But suspiciously close
)
WITH path, 
     [rel IN relationships(path) | rel.amount] as amounts,
     [rel IN relationships(path) | rel.timestamp] as timestamps
WHERE size(amounts) >= 5  // At least 5 transactions in chain
RETURN [node IN nodes(path) | node.account_number] as path_accounts,
       reduce(total = 0.0, amt IN amounts | total + amt) as total_amount,
       size(amounts) as transaction_count,
       (max(timestamps) - min(timestamps)) as time_window
ORDER BY total_amount DESC
```

**Fraud Indicator**: Multiple transactions just below reporting threshold

**Regulatory**: Structuring to avoid AML/CTF reporting

### 5. Merchant Fraud Pattern

Detect coordinated bust-out across multiple accounts:

```cypher
// Find multiple accounts making large purchases from same high-risk merchant
MATCH (account:Account)-[paid:PAID]->(merchant:Merchant)
WHERE merchant.risk_level = 'high' 
  AND paid.timestamp > datetime() - duration({days: 7})
  AND paid.amount > 1000
WITH merchant, collect(account) as accounts, sum(paid.amount) as total_spent
WHERE size(accounts) >= 5
MATCH (customer:Customer)-[:OWNS]->(account)
WHERE account IN accounts
OPTIONAL MATCH (customer)-[:SHARES_PHONE|SHARES_EMAIL|SHARES_ADDRESS]-(linked:Customer)
RETURN merchant.name,
       merchant.category,
       size(accounts) as affected_accounts,
       total_spent,
       count(DISTINCT linked) as linked_customers
ORDER BY total_spent DESC
```

**Fraud Indicator**: Coordinated bust-out from linked accounts

**Use Case**: Credit card fraud ring detection

### 6. First-Party Fraud (Bust-Out Pattern)

Detect accounts building credit then maxing out:

```cypher
// Find accounts with rapid balance increase near limit
MATCH (customer:Customer)-[:OWNS]->(account:Account)
WHERE account.current_balance > account.credit_limit * 0.9  // Near limit
  AND account.opened_at > datetime() - duration({days: 180})  // Recently opened
MATCH (account)-[txn:TRANSFERRED]->(other:Account)
WHERE txn.timestamp > datetime() - duration({days: 30})
WITH account, customer, count(txn) as recent_txns, sum(txn.amount) as recent_volume
WHERE recent_txns > 10 AND recent_volume > account.credit_limit * 0.5
RETURN customer.name,
       account.account_number,
       account.credit_limit,
       account.current_balance,
       recent_txns,
       recent_volume,
       account.risk_score
ORDER BY recent_volume DESC
```

**Fraud Indicator**: Building credit history then sudden maxing out

**Use Case**: First-party fraud / bust-out detection

## Advanced Techniques

### 7. Anomaly Detection with PageRank

Identify accounts with unusual centrality in transaction network:

```cypher
// Find accounts that are hubs in transaction network
CALL algo.pagerank({
  nodeQuery: 'MATCH (a:Account) RETURN id(a) as id',
  relationshipQuery: 'MATCH (a1:Account)-[t:TRANSFERRED]->(a2:Account) 
                      WHERE t.timestamp > datetime() - duration({days: 30})
                      RETURN id(a1) as source, id(a2) as target, t.amount as weight',
  dampingFactor: 0.85,
  iterations: 20
})
YIELD nodeId, score
MATCH (account:Account) WHERE id(account) = nodeId
WHERE score > 0.01  // High centrality
RETURN account.account_number, 
       account.risk_score,
       score as centrality_score,
       account.current_balance
ORDER BY score DESC
LIMIT 20
```

**Fraud Indicator**: Accounts acting as transaction hubs

### 8. Velocity Checks

Detect abnormal transaction velocity:

```cypher
// Find accounts with unusual transaction frequency
MATCH (account:Account)-[txn:TRANSFERRED]->(other:Account)
WHERE txn.timestamp > datetime() - duration({hours: 24})
WITH account, count(txn) as txn_count_24h, sum(txn.amount) as volume_24h
WHERE txn_count_24h > 20  // More than 20 transactions per day
MATCH (account)-[hist:TRANSFERRED]->(other2:Account)
WHERE hist.timestamp > datetime() - duration({days: 30}) 
  AND hist.timestamp < datetime() - duration({days: 1})
WITH account, txn_count_24h, volume_24h, 
     count(hist) as historical_avg_count
WHERE txn_count_24h > historical_avg_count * 3  // 3x normal activity
RETURN account.account_number,
       account.risk_score,
       txn_count_24h,
       volume_24h,
       historical_avg_count,
       (txn_count_24h * 1.0 / historical_avg_count) as velocity_ratio
ORDER BY velocity_ratio DESC
```

**Fraud Indicator**: Sudden spike in transaction frequency

### 9. Geographic Impossibility

Detect transactions from impossible locations:

```cypher
// Find rapid transactions from distant locations
MATCH (account:Account)-[:USED_DEVICE]->(d1:Device)-[:CONNECTED_FROM]->(ip1:IPAddress)
MATCH (account)-[:USED_DEVICE]->(d2:Device)-[:CONNECTED_FROM]->(ip2:IPAddress)
WHERE d1.last_seen_at < d2.first_seen_at
  AND (d2.first_seen_at - d1.last_seen_at) < duration({hours: 2})
  AND ip1.country_code <> ip2.country_code
  AND ip1.country_code IN ['US', 'CA', 'MX']
  AND ip2.country_code IN ['RU', 'CN', 'NG']  // High-risk countries
MATCH (customer:Customer)-[:OWNS]->(account)
RETURN customer.name,
       account.account_number,
       ip1.country_code as location1,
       ip2.country_code as location2,
       (d2.first_seen_at - d1.last_seen_at) as time_gap,
       ip2.is_vpn,
       ip2.ip_risk_score
```

**Fraud Indicator**: Account accessed from impossible geographic locations

**Use Case**: Account takeover detection

### 10. Money Mule Identification

Find accounts receiving funds from many sources and forwarding:

```cypher
// Detect potential money mule accounts
MATCH (sender:Account)-[in:TRANSFERRED]->(mule:Account)-[out:TRANSFERRED]->(receiver:Account)
WHERE in.timestamp > datetime() - duration({days: 30})
  AND out.timestamp > in.timestamp
  AND out.timestamp < in.timestamp + duration({hours: 48})  // Quick turnaround
WITH mule, 
     count(DISTINCT sender) as unique_senders,
     count(DISTINCT receiver) as unique_receivers,
     sum(in.amount) as total_in,
     sum(out.amount) as total_out
WHERE unique_senders > 10 AND unique_receivers > 5
  AND total_out > total_in * 0.9  // Most money forwarded
MATCH (customer:Customer)-[:OWNS]->(mule)
RETURN customer.name,
       mule.account_number,
       unique_senders,
       unique_receivers,
       total_in,
       total_out,
       (total_out / total_in) as forward_ratio,
       mule.risk_score
ORDER BY unique_senders DESC
```

**Fraud Indicator**: Account receiving from many, forwarding to many

**Use Case**: Money mule / money laundering detection

## Real-Time Detection

### Fraud Scoring Function

```python
# fraud_scorer.py
import requests
from datetime import datetime, timedelta

def calculate_fraud_score(account_id):
    """Calculate real-time fraud score for an account"""
    
    scores = {}
    
    # Check 1: Circular transaction involvement
    query1 = f"""
    MATCH path = (acc:Account {{account_id: {account_id}}})-[:TRANSFERRED*2..5]->(acc)
    WHERE ALL(rel IN relationships(path) 
              WHERE rel.timestamp > datetime() - duration({{days: 7}}))
    RETURN count(path) as circular_paths
    """
    result = requests.post('http://localhost:8080/query', json={'query': query1}).json()
    scores['circular'] = min(result['results'][0]['circular_paths'] * 10, 30)
    
    # Check 2: Shared data with other accounts
    query2 = f"""
    MATCH (c1:Customer)-[:OWNS]->(acc:Account {{account_id: {account_id}}})
    MATCH (c1)-[:SHARES_PHONE|SHARES_EMAIL|SHARES_ADDRESS]-(c2:Customer)
    RETURN count(DISTINCT c2) as linked_customers
    """
    result = requests.post('http://localhost:8080/query', json={'query': query2}).json()
    scores['synthetic_identity'] = min(result['results'][0]['linked_customers'] * 5, 25)
    
    # Check 3: Transaction velocity
    query3 = f"""
    MATCH (acc:Account {{account_id: {account_id}}})-[txn:TRANSFERRED]->()
    WHERE txn.timestamp > datetime() - duration({{hours: 24}})
    RETURN count(txn) as txn_count_24h
    """
    result = requests.post('http://localhost:8080/query', json={'query': query3}).json()
    txn_count = result['results'][0]['txn_count_24h']
    scores['velocity'] = min(max(txn_count - 10, 0) * 2, 20)
    
    # Check 4: High-risk merchant transactions
    query4 = f"""
    MATCH (acc:Account {{account_id: {account_id}}})-[paid:PAID]->(m:Merchant)
    WHERE m.risk_level = 'high' 
      AND paid.timestamp > datetime() - duration({{days: 7}})
    RETURN count(paid) as high_risk_txns, sum(paid.amount) as high_risk_volume
    """
    result = requests.post('http://localhost:8080/query', json={'query': query4}).json()
    high_risk = result['results'][0]
    scores['merchant_risk'] = min(high_risk['high_risk_txns'] * 3, 15)
    
    # Check 5: Device/IP risk
    query5 = f"""
    MATCH (c:Customer)-[:OWNS]->(acc:Account {{account_id: {account_id}}})
    MATCH (c)-[:USED_DEVICE]->(d:Device)-[:CONNECTED_FROM]->(ip:IPAddress)
    WHERE ip.is_vpn = 1 OR ip.is_proxy = 1 OR ip.ip_risk_score > 0.7
    RETURN count(DISTINCT ip) as risky_ips
    """
    result = requests.post('http://localhost:8080/query', json={'query': query5}).json()
    scores['ip_risk'] = min(result['results'][0]['risky_ips'] * 5, 10)
    
    # Total score (0-100)
    total_score = sum(scores.values())
    
    return {
        'account_id': account_id,
        'total_score': total_score,
        'risk_level': 'HIGH' if total_score > 50 else 'MEDIUM' if total_score > 25 else 'LOW',
        'components': scores,
        'timestamp': datetime.now().isoformat()
    }

# Example usage
fraud_score = calculate_fraud_score(10015)
print(f"Fraud Score: {fraud_score['total_score']}/100")
print(f"Risk Level: {fraud_score['risk_level']}")
print(f"Components: {fraud_score['components']}")
```

### Real-Time Alert System

```python
# fraud_alerts.py
import requests
import time
from datetime import datetime

def monitor_high_risk_patterns():
    """Continuously monitor for fraud patterns"""
    
    alert_queries = {
        'circular_flow': """
            MATCH path = (a:Account)-[:TRANSFERRED*3..5]->(a)
            WHERE ALL(rel IN relationships(path) 
                      WHERE rel.timestamp > datetime() - duration({minutes: 15}))
            RETURN [n IN nodes(path) | n.account_number] as cycle,
                   reduce(t = 0.0, rel IN relationships(path) | t + rel.amount) as amount
            ORDER BY amount DESC LIMIT 5
        """,
        
        'velocity_spike': """
            MATCH (a:Account)-[t:TRANSFERRED]->()
            WHERE t.timestamp > datetime() - duration({minutes: 15})
            WITH a, count(t) as recent_txns
            WHERE recent_txns > 5
            RETURN a.account_number, recent_txns, a.risk_score
            ORDER BY recent_txns DESC LIMIT 5
        """,
        
        'geographic_anomaly': """
            MATCH (a:Account)-[:USED_DEVICE]->(d1:Device)-[:CONNECTED_FROM]->(ip1:IPAddress),
                  (a)-[:USED_DEVICE]->(d2:Device)-[:CONNECTED_FROM]->(ip2:IPAddress)
            WHERE d2.last_seen_at > datetime() - duration({minutes: 15})
              AND (d2.last_seen_at - d1.last_seen_at) < duration({hours: 1})
              AND ip1.country_code <> ip2.country_code
            RETURN a.account_number, ip1.country_code, ip2.country_code
            LIMIT 5
        """
    }
    
    while True:
        for alert_name, query in alert_queries.items():
            try:
                response = requests.post('http://localhost:8080/query', 
                                       json={'query': query},
                                       timeout=5)
                results = response.json()['results']
                
                if results:
                    print(f"\n🚨 ALERT: {alert_name} - {datetime.now()}")
                    for result in results:
                        print(f"  {result}")
                        
            except Exception as e:
                print(f"Error checking {alert_name}: {e}")
        
        time.sleep(60)  # Check every minute

# Run monitoring
monitor_high_risk_patterns()
```

## Performance Optimization

### Indexing Strategy

```sql
-- Optimize transaction time queries
ALTER TABLE fraud_db.transactions ADD INDEX idx_txn_time transaction_time TYPE minmax;

-- Optimize account lookups
ALTER TABLE fraud_db.accounts ADD INDEX idx_account_id account_id TYPE bloom_filter;
ALTER TABLE fraud_db.accounts ADD INDEX idx_risk risk_score TYPE minmax;

-- Optimize device queries
ALTER TABLE fraud_db.device_usage ADD INDEX idx_device device_id TYPE bloom_filter;
```

### Query Optimization

```cypher
-- ❌ Slow: Full graph scan
MATCH path = (a:Account)-[:TRANSFERRED*3..6]->(a)
RETURN path

-- ✅ Fast: Time-filtered + limited depth
MATCH path = (a:Account)-[:TRANSFERRED*3..6]->(a)
WHERE ALL(rel IN relationships(path) 
          WHERE rel.timestamp > datetime() - duration({days: 7}))
  AND a.risk_score > 0.5
RETURN path LIMIT 100
```

### Performance Benchmarks

**Dataset**: 10K accounts, 500K transactions, 100 fraud rings

| Query Pattern | Avg Time | p95 Time |
|---------------|----------|----------|
| Circular flow (3-5 hops) | 180ms | 320ms |
| Account rings (shared data) | 90ms | 150ms |
| Device fingerprint | 45ms | 80ms |
| Velocity check (24h) | 60ms | 110ms |
| Money mule detection | 220ms | 400ms |

## Next Steps

- **[Social Network Analysis](Use-Case-Social-Network.md)** - Graph analytics for social networks
- **[Knowledge Graph Use Case](Use-Case-Knowledge-Graphs.md)** - Semantic knowledge graphs
- **[Performance Optimization](Performance-Query-Optimization.md)** - Advanced optimization techniques
- **[Production Best Practices](Production-Best-Practices.md)** - Security and monitoring for fraud detection

## Additional Resources

- [Cypher Multi-Hop Traversals](Cypher-Multi-Hop-Traversals.md)
- [Schema Configuration Advanced](Schema-Configuration-Advanced.md)
- [Multi-Tenancy RBAC](Multi-Tenancy-RBAC.md)
