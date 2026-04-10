DROP TABLE IF EXISTS car_sales;
CREATE TABLE car_sales (
    id SERIAL PRIMARY KEY,
    car_model VARCHAR(100),
    seller_name VARCHAR(100),
    buyer_name VARCHAR(100),
    price NUMERIC(10,2),
    sale_date DATE,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO car_sales (car_model, seller_name, buyer_name, price, sale_date, last_update)
VALUES
('Toyota Corolla', 'Ahmed', 'Fatma', 250000.00, '2024-09-01', NOW() - INTERVAL '5 days'),
('Hyundai Elantra', 'Omar', 'Sara', 300000.00, '2024-09-05', NOW() - INTERVAL '3 days'),
('Kia Sportage', 'Mona', 'Ali', 600000.00, '2024-09-10', NOW() - INTERVAL '1 day'),
('Nissan Sunny', 'Youssef', 'Hany', 280000.00, '2024-09-12', NOW()),
('Chevrolet Optra', 'Hassan', 'Nada', 310000.00, '2024-09-14', NOW());

CREATE INDEX idx_car_sales_last_update ON car_sales(last_update);
