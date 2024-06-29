CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2),
    quantity_in_stock INT,
    date_added DATE
);



INSERT INTO products (product_id, product_name, category, price, quantity_in_stock, date_added)
VALUES
(1, 'Laptop', 'Electronics', 999.99, 50, '2024-01-15'),
(2, 'Smartphone', 'Electronics', 599.99, 200, '2024-02-10'),
(3, 'Desk Chair', 'Furniture', 149.99, 100, '2024-03-05'),
(4, 'Bluetooth Speaker', 'Audio', 79.99, 150, '2024-04-12'),
(5, 'Electric Kettle', 'Home Appliances', 29.99, 75, '2024-05-20'),
(6, 'Gaming Mouse', 'Accessories', 49.99, 120, '2024-06-18'),
(7, 'Monitor', 'Electronics', 199.99, 80, '2024-07-22'),
(8, 'Office Desk', 'Furniture', 299.99, 60, '2024-08-30'),
(9, 'Headphones', 'Audio', 89.99, 130, '2024-09-15'),
(10, 'Blender', 'Home Appliances', 39.99, 50, '2024-10-10');