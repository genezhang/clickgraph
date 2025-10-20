-- E-commerce Analytics Sample Data
USE ecommerce;

-- Insert customers (representing different personas)
INSERT INTO customers VALUES 
    (1, 'alice.johnson@email.com', 'Alice', 'Johnson', 28, 'F', 'USA', 'New York', '2023-01-15', 1250.00, 1),
    (2, 'bob.smith@email.com', 'Bob', 'Smith', 34, 'M', 'Canada', 'Toronto', '2023-02-20', 890.50, 0),
    (3, 'carol.brown@email.com', 'Carol', 'Brown', 42, 'F', 'UK', 'London', '2023-01-10', 2100.75, 1),
    (4, 'david.wilson@email.com', 'David', 'Wilson', 25, 'M', 'Australia', 'Sydney', '2023-03-05', 450.25, 0),
    (5, 'emma.davis@email.com', 'Emma', 'Davis', 31, 'F', 'Germany', 'Berlin', '2023-02-28', 1680.00, 1),
    (6, 'frank.miller@email.com', 'Frank', 'Miller', 29, 'M', 'USA', 'San Francisco', '2023-01-25', 750.30, 0),
    (7, 'grace.lee@email.com', 'Grace', 'Lee', 26, 'F', 'Japan', 'Tokyo', '2023-03-10', 920.80, 0),
    (8, 'henry.taylor@email.com', 'Henry', 'Taylor', 38, 'M', 'France', 'Paris', '2023-02-15', 1450.60, 1);

-- Insert products across different categories
INSERT INTO products VALUES
    (101, 'iPhone 15 Pro', 'Electronics', 'Apple', 999.99, 4.5, 1250, 1, '2023-09-15'),
    (102, 'Samsung Galaxy S24', 'Electronics', 'Samsung', 849.99, 4.3, 890, 1, '2024-01-20'),
    (103, 'Sony WH-1000XM5', 'Electronics', 'Sony', 399.99, 4.7, 2100, 1, '2023-05-10'),
    (104, 'MacBook Pro M3', 'Electronics', 'Apple', 1999.99, 4.6, 650, 1, '2023-11-01'),
    (105, 'Nike Air Max', 'Fashion', 'Nike', 129.99, 4.2, 450, 1, '2023-06-15'),
    (106, 'Levis 501 Jeans', 'Fashion', 'Levis', 69.99, 4.1, 320, 1, '2023-04-20'),
    (107, 'The Great Gatsby', 'Books', 'Scribner', 12.99, 4.4, 1890, 1, '2023-01-01'),
    (108, 'Instant Pot Duo', 'Home & Kitchen', 'Instant Pot', 79.99, 4.5, 3200, 1, '2023-03-15'),
    (109, 'Dyson V15 Vacuum', 'Home & Kitchen', 'Dyson', 549.99, 4.4, 780, 1, '2023-07-20'),
    (110, 'Fitbit Charge 6', 'Electronics', 'Fitbit', 159.99, 4.0, 1100, 1, '2023-10-05');

-- Insert orders (purchase history)
INSERT INTO orders VALUES
    (1001, 1, 101, 1, 999.99, 999.99, '2024-01-20', '2024-01-20 14:30:00', 'delivered'),
    (1002, 1, 103, 1, 399.99, 399.99, '2024-02-15', '2024-02-15 10:15:00', 'delivered'),
    (1003, 2, 105, 2, 129.99, 259.98, '2024-01-25', '2024-01-25 16:45:00', 'delivered'),
    (1004, 3, 104, 1, 1999.99, 1999.99, '2024-02-10', '2024-02-10 09:20:00', 'delivered'),
    (1005, 3, 108, 1, 79.99, 79.99, '2024-02-12', '2024-02-12 11:30:00', 'delivered'),
    (1006, 4, 107, 3, 12.99, 38.97, '2024-01-30', '2024-01-30 13:15:00', 'delivered'),
    (1007, 5, 102, 1, 849.99, 849.99, '2024-03-05', '2024-03-05 15:45:00', 'shipped'),
    (1008, 5, 106, 2, 69.99, 139.98, '2024-03-06', '2024-03-06 12:00:00', 'delivered'),
    (1009, 6, 110, 1, 159.99, 159.99, '2024-02-20', '2024-02-20 17:30:00', 'delivered'),
    (1010, 7, 109, 1, 549.99, 549.99, '2024-03-15', '2024-03-15 14:00:00', 'pending'),
    (1011, 8, 101, 1, 999.99, 999.99, '2024-03-01', '2024-03-01 10:45:00', 'delivered'),
    (1012, 1, 108, 1, 79.99, 79.99, '2024-03-20', '2024-03-20 16:20:00', 'delivered');

-- Insert reviews
INSERT INTO reviews VALUES
    (2001, 1, 101, 1001, 5, 'Amazing phone! Camera quality is outstanding.', '2024-01-25', 15),
    (2002, 1, 103, 1002, 5, 'Best noise cancellation I''ve ever experienced.', '2024-02-20', 23),
    (2003, 2, 105, 1003, 4, 'Comfortable and stylish. Great for running.', '2024-02-01', 8),
    (2004, 3, 104, 1004, 5, 'MacBook Pro is incredibly fast. Worth every penny.', '2024-02-15', 31),
    (2005, 3, 108, 1005, 4, 'Makes cooking so much easier. Highly recommended.', '2024-02-17', 12),
    (2006, 4, 107, 1006, 5, 'Classic literature at its finest. Timeless story.', '2024-02-05', 6),
    (2007, 5, 102, 1007, 4, 'Great Android phone. Battery life could be better.', '2024-03-10', 9),
    (2008, 6, 110, 1009, 3, 'Good fitness tracker but app needs improvement.', '2024-02-25', 4),
    (2009, 8, 101, 1011, 5, 'Upgraded from iPhone 12. Significant improvement!', '2024-03-05', 18);

-- Insert category hierarchy
INSERT INTO category_hierarchy VALUES
    ('Technology', 'Electronics', 1),
    ('Technology', 'Computers', 1), 
    ('Lifestyle', 'Fashion', 1),
    ('Lifestyle', 'Home & Kitchen', 1),
    ('Education', 'Books', 1),
    ('Electronics', 'Smartphones', 2),
    ('Electronics', 'Audio', 2),
    ('Fashion', 'Footwear', 2),
    ('Fashion', 'Clothing', 2);