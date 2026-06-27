INSERT INTO users (full_name, city) VALUES
  ('Ahmet Yilmaz', 'Istanbul'),
  ('Elif Demir', 'Ankara'),
  ('Mehmet Kaya', 'Izmir'),
  ('Zeynep Sahin', 'Bursa'),
  ('Can Aydin', 'Antalya');

INSERT INTO products (product_id, name, category, price) VALUES
  (1, 'Kablosuz Kulaklik', 'Elektronik', 1299.90),
  (2, 'Bluetooth Hoparlor', 'Elektronik', 899.50),
  (3, 'Kosu Ayakkabisi', 'Ayakkabi', 1599.00),
  (4, 'Sirt Cantasi', 'Aksesuar', 749.90),
  (5, 'Akilli Saat', 'Elektronik', 3499.00),
  (6, 'Pamuklu Tisort', 'Giyim', 299.90),
  (7, 'Termos', 'Aksesuar', 449.00),
  (8, 'Yoga Mati', 'Spor', 399.90);

INSERT INTO inventory (product_id, stock_qty)
SELECT product_id, 100 FROM products;