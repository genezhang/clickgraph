#!/usr/bin/env python3
"""
ClickGraph Benchmark Data Setup

Sets up benchmark datasets for ClickGraph performance evaluation.

Supported datasets:
- social: Social network with users, follows, posts, and likes
- ecommerce: E-commerce with customers, products, orders, and reviews
- scale: Scalable datasets with configurable sizes

Usage:
    python setup_benchmark_data.py --dataset social --size small
    python setup_benchmark_data.py --dataset ecommerce --size medium
    python setup_benchmark_data.py --dataset all --size small
"""

import argparse
import random
import string
from datetime import datetime, timedelta
import requests
import time

class BenchmarkDataGenerator:
    """Generate benchmark data for ClickGraph testing."""

    def __init__(self, clickhouse_url: str = "http://localhost:8123", database: str = "default"):
        self.clickhouse_url = clickhouse_url
        self.database = database
        self.session = requests.Session()

    def execute_sql(self, sql: str) -> bool:
        """Execute SQL query against ClickHouse."""
        try:
            response = self.session.post(
                f"{self.clickhouse_url}/",
                params={"database": self.database, "query": sql},
                timeout=30
            )
            return response.status_code == 200
        except Exception as e:
            print(f"❌ SQL execution failed: {e}")
            return False

    def setup_social_network(self, size: str = "small"):
        """Set up social network dataset."""

        print("🚀 Setting up Social Network dataset...")

        # Size configurations
        sizes = {
            "small": {"users": 1000, "follows_multiplier": 5, "posts_multiplier": 2},
            "medium": {"users": 10000, "follows_multiplier": 8, "posts_multiplier": 3},
            "large": {"users": 50000, "follows_multiplier": 10, "posts_multiplier": 5}
        }

        config = sizes.get(size, sizes["small"])
        num_users = config["users"]
        num_follows = num_users * config["follows_multiplier"]
        num_posts = num_users * config["posts_multiplier"]

        print(f"📊 Generating: {num_users} users, {num_follows} follows, {num_posts} posts")

        # Create tables
        self.execute_sql(f"""
            CREATE DATABASE IF NOT EXISTS social_bench;
            USE social_bench;

            DROP TABLE IF EXISTS user_follows;
            DROP TABLE IF EXISTS posts;
            DROP TABLE IF EXISTS post_likes;
            DROP TABLE IF EXISTS users;

            CREATE TABLE users (
                user_id UInt32,
                full_name String,
                email_address String,
                registration_date Date,
                is_active UInt8,
                country String,
                city String
            ) ENGINE = MergeTree()
            ORDER BY user_id;

            CREATE TABLE user_follows (
                follower_id UInt32,
                followed_id UInt32,
                follow_date Date
            ) ENGINE = MergeTree()
            ORDER BY (follower_id, followed_id);

            CREATE TABLE posts (
                post_id UInt32,
                author_id UInt32,
                post_title String,
                post_content String,
                post_date DateTime
            ) ENGINE = MergeTree()
            ORDER BY post_id;

            CREATE TABLE post_likes (
                user_id UInt32,
                post_id UInt32,
                like_date DateTime
            ) ENGINE = MergeTree()
            ORDER BY (user_id, post_id);
        """)

        # Generate and insert users
        print("👥 Generating users...")
        users_data = []
        countries = ["USA", "UK", "Canada", "Germany", "France", "Australia"]
        cities = ["New York", "London", "Toronto", "Berlin", "Paris", "Sydney"]

        for i in range(1, num_users + 1):
            first_name = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=random.randint(3, 8)))
            last_name = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=random.randint(3, 10)))
            full_name = f"{first_name} {last_name}"
            email = f"{first_name.lower()}.{last_name.lower()}@example.com"
            reg_date = (datetime.now() - timedelta(days=random.randint(0, 365*2))).date()
            is_active = random.choice([0, 1])
            country = random.choice(countries)
            city = random.choice(cities)

            users_data.append(f"({i}, '{full_name}', '{email}', '{reg_date}', {is_active}, '{country}', '{city}')")

            if len(users_data) >= 1000:  # Batch insert
                self.execute_sql(f"INSERT INTO users VALUES {','.join(users_data)};")
                users_data = []

        if users_data:
            self.execute_sql(f"INSERT INTO users VALUES {','.join(users_data)};")

        # Generate follows
        print("🤝 Generating follows...")
        follows_data = []
        for _ in range(num_follows):
            follower = random.randint(1, num_users)
            followed = random.randint(1, num_users)
            if follower != followed:  # No self-follows
                follow_date = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
                follows_data.append(f"({follower}, {followed}, '{follow_date}')")

                if len(follows_data) >= 1000:
                    self.execute_sql(f"INSERT INTO user_follows VALUES {','.join(follows_data)};")
                    follows_data = []

        if follows_data:
            self.execute_sql(f"INSERT INTO user_follows VALUES {','.join(follows_data)};")

        # Generate posts
        print("📝 Generating posts...")
        posts_data = []
        post_titles = [
            "My thoughts on technology", "Weekend plans", "Favorite recipes",
            "Travel memories", "Book recommendations", "Movie reviews",
            "Career updates", "Fitness journey", "Music discoveries"
        ]

        for i in range(1, num_posts + 1):
            author_id = random.randint(1, num_users)
            title = random.choice(post_titles)
            content = ''.join(random.choices(string.ascii_letters + ' ', k=random.randint(50, 500)))
            content = content.replace("'", "''")  # Escape quotes
            post_date = datetime.now() - timedelta(minutes=random.randint(0, 365*24*60))

            posts_data.append(f"({i}, {author_id}, '{title}', '{content}', '{post_date}')")

            if len(posts_data) >= 500:
                self.execute_sql(f"INSERT INTO posts VALUES {','.join(posts_data)};")
                posts_data = []

        if posts_data:
            self.execute_sql(f"INSERT INTO posts VALUES {','.join(posts_data)};")

        print("✅ Social network dataset setup complete!")

    def setup_ecommerce(self, size: str = "small"):
        """Set up e-commerce dataset."""

        print("🛒 Setting up E-commerce dataset...")

        sizes = {
            "small": {"customers": 1000, "products": 500, "orders_multiplier": 3},
            "medium": {"customers": 10000, "products": 2000, "orders_multiplier": 5},
            "large": {"customers": 50000, "products": 10000, "orders_multiplier": 8}
        }

        config = sizes.get(size, sizes["small"])
        num_customers = config["customers"]
        num_products = config["products"]
        num_orders = num_customers * config["orders_multiplier"]

        print(f"📊 Generating: {num_customers} customers, {num_products} products, {num_orders} orders")

        # Create tables
        self.execute_sql(f"""
            CREATE DATABASE IF NOT EXISTS ecommerce_bench;
            USE ecommerce_bench;

            DROP TABLE IF EXISTS reviews;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS products;
            DROP TABLE IF EXISTS customers;

            CREATE TABLE customers (
                customer_id UInt32,
                email String,
                first_name String,
                last_name String,
                age UInt8,
                gender String,
                country String,
                city String,
                registration_date Date,
                total_spent Float64,
                is_premium UInt8
            ) ENGINE = MergeTree()
            ORDER BY customer_id;

            CREATE TABLE products (
                product_id UInt32,
                name String,
                category String,
                brand String,
                price Float64,
                rating Float32,
                num_reviews UInt32,
                in_stock UInt8,
                created_date Date
            ) ENGINE = MergeTree()
            ORDER BY product_id;

            CREATE TABLE orders (
                order_id UInt32,
                customer_id UInt32,
                product_id UInt32,
                quantity UInt16,
                unit_price Float64,
                total_amount Float64,
                order_date Date,
                order_time DateTime,
                status String
            ) ENGINE = MergeTree()
            ORDER BY order_id;

            CREATE TABLE reviews (
                review_id UInt32,
                customer_id UInt32,
                product_id UInt32,
                order_id UInt32,
                rating UInt8,
                review_text String,
                review_date Date,
                helpful_votes UInt32
            ) ENGINE = MergeTree()
            ORDER BY review_id;
        """)

        # Generate customers
        print("👥 Generating customers...")
        customers_data = []
        countries = ["USA", "UK", "Canada", "Germany", "France", "Australia"]
        cities = ["New York", "London", "Toronto", "Berlin", "Paris", "Sydney"]

        for i in range(1, num_customers + 1):
            first_name = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=random.randint(3, 8)))
            last_name = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=random.randint(3, 10)))
            email = f"{first_name.lower()}.{last_name.lower()}@example.com"
            age = random.randint(18, 80)
            gender = random.choice(["M", "F"])
            country = random.choice(countries)
            city = random.choice(cities)
            reg_date = (datetime.now() - timedelta(days=random.randint(0, 365*3))).date()
            total_spent = round(random.uniform(0, 10000), 2)
            is_premium = random.choice([0, 1])

            customers_data.append(f"({i}, '{email}', '{first_name}', '{last_name}', {age}, '{gender}', '{country}', '{city}', '{reg_date}', {total_spent}, {is_premium})")

            if len(customers_data) >= 1000:
                self.execute_sql(f"INSERT INTO customers VALUES {','.join(customers_data)};")
                customers_data = []

        if customers_data:
            self.execute_sql(f"INSERT INTO customers VALUES {','.join(customers_data)};")

        # Generate products
        print("📦 Generating products...")
        products_data = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports", "Beauty"]
        brands = ["Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Amazon Basics"]

        for i in range(1, num_products + 1):
            name = f"Product {i}"
            category = random.choice(categories)
            brand = random.choice(brands)
            price = round(random.uniform(10, 1000), 2)
            rating = round(random.uniform(1, 5), 1)
            num_reviews = random.randint(0, 1000)
            in_stock = random.choice([0, 1])
            created_date = (datetime.now() - timedelta(days=random.randint(0, 365))).date()

            products_data.append(f"({i}, '{name}', '{category}', '{brand}', {price}, {rating}, {num_reviews}, {in_stock}, '{created_date}')")

            if len(products_data) >= 1000:
                self.execute_sql(f"INSERT INTO products VALUES {','.join(products_data)};")
                products_data = []

        if products_data:
            self.execute_sql(f"INSERT INTO products VALUES {','.join(products_data)};")

        # Generate orders
        print("🛍️  Generating orders...")
        orders_data = []
        statuses = ["pending", "shipped", "delivered", "cancelled"]

        for i in range(1, num_orders + 1):
            customer_id = random.randint(1, num_customers)
            product_id = random.randint(1, num_products)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10, 1000), 2)
            total_amount = round(quantity * unit_price, 2)
            order_date = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
            order_time = datetime.now() - timedelta(minutes=random.randint(0, 365*24*60))
            status = random.choice(statuses)

            orders_data.append(f"({i}, {customer_id}, {product_id}, {quantity}, {unit_price}, {total_amount}, '{order_date}', '{order_time}', '{status}')")

            if len(orders_data) >= 1000:
                self.execute_sql(f"INSERT INTO orders VALUES {','.join(orders_data)};")
                orders_data = []

        if orders_data:
            self.execute_sql(f"INSERT INTO orders VALUES {','.join(orders_data)};")

        print("✅ E-commerce dataset setup complete!")

    def create_yaml_configs(self):
        """Create YAML configuration files for the benchmark datasets."""

        print("📝 Creating YAML configuration files...")

        # Social network config
        social_config = """
graph_schema:
  nodes:
    - label: User
      table: users
      id_column: user_id
      properties:
        user_id: user_id
        full_name: full_name
        email_address: email_address
        registration_date: registration_date
        is_active: is_active
        country: country
        city: city

  relationships:
    - type: FOLLOWS
      table: user_follows
      from_column: follower_id
      to_column: followed_id
      properties:
        follow_date: follow_date
"""

        with open("social_benchmark.yaml", "w") as f:
            f.write(social_config)

        # E-commerce config
        ecommerce_config = """
graph_schema:
  nodes:
    - label: Customer
      table: customers
      id_column: customer_id
      properties:
        customer_id: customer_id
        email: email
        first_name: first_name
        last_name: last_name
        age: age
        gender: gender
        country: country
        city: city
        registration_date: registration_date
        total_spent: total_spent
        is_premium: is_premium

    - label: Product
      table: products
      id_column: product_id
      properties:
        product_id: product_id
        name: name
        category: category
        brand: brand
        price: price
        rating: rating
        num_reviews: num_reviews
        in_stock: in_stock
        created_date: created_date

  relationships:
    - type: PURCHASED
      table: orders
      from_column: customer_id
      to_column: product_id
      properties:
        order_id: order_id
        quantity: quantity
        unit_price: unit_price
        total_amount: total_amount
        order_date: order_date
        order_time: order_time
        status: status
"""

        with open("ecommerce_benchmark.yaml", "w") as f:
            f.write(ecommerce_config)

        print("✅ YAML configurations created!")

def main():
    parser = argparse.ArgumentParser(description="ClickGraph Benchmark Data Setup")
    parser.add_argument("--dataset", choices=["social", "ecommerce", "all"],
                       default="social", help="Dataset to set up")
    parser.add_argument("--size", choices=["small", "medium", "large"],
                       default="small", help="Dataset size")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123",
                       help="ClickHouse server URL")

    args = parser.parse_args()

    generator = BenchmarkDataGenerator(args.clickhouse_url)

    # Test connection
    if not generator.execute_sql("SELECT 1"):
        print("❌ Cannot connect to ClickHouse server")
        print("💡 Make sure ClickHouse is running on the specified URL")
        return

    print("✅ Connected to ClickHouse server")

    # Setup datasets
    if args.dataset in ["social", "all"]:
        generator.setup_social_network(args.size)

    if args.dataset in ["ecommerce", "all"]:
        generator.setup_ecommerce(args.size)

    # Create YAML configs
    generator.create_yaml_configs()

    print("\n🎉 Benchmark data setup complete!")
    print("📁 Configuration files created: social_benchmark.yaml, ecommerce_benchmark.yaml")
    print("🗄️  Databases created: social_bench, ecommerce_bench")
    print("\n🚀 Ready to run benchmarks with:")
    print("   python benchmark.py --dataset social --config social_benchmark.yaml")
    print("   python benchmark.py --dataset ecommerce --config ecommerce_benchmark.yaml")

if __name__ == "__main__":
    main()