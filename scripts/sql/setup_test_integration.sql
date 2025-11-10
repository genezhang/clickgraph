-- Setup test data for integration tests
INSERT INTO test_integration.users VALUES (1, 'Alice', 30);
INSERT INTO test_integration.users VALUES (2, 'Bob', 25);
INSERT INTO test_integration.users VALUES (3, 'Charlie', 35);
INSERT INTO test_integration.users VALUES (4, 'Diana', 28);
INSERT INTO test_integration.users VALUES (5, 'Eve', 32);

INSERT INTO test_integration.follows VALUES (1, 2, '2023-01-01');
INSERT INTO test_integration.follows VALUES (1, 3, '2023-01-15');
INSERT INTO test_integration.follows VALUES (2, 3, '2023-02-01');
INSERT INTO test_integration.follows VALUES (3, 4, '2023-02-15');
INSERT INTO test_integration.follows VALUES (4, 5, '2023-03-01');
INSERT INTO test_integration.follows VALUES (2, 4, '2023-03-15');
