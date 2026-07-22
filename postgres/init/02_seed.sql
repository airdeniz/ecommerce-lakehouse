-- Seed data. Enriched to a realistic scale for richer analytics:
-- a few hundred customers with varied cities and ~50 products across the
-- existing categories. The original named users/products are kept first for
-- continuity, then the rest are generated programmatically with generate_series
-- so this file stays compact. Schema is unchanged.

-- A handful of explicit, human-named users (kept from the original seed).
INSERT INTO users (full_name, city) VALUES
  ('Ahmet Yilmaz', 'Istanbul'),
  ('Elif Demir', 'Ankara'),
  ('Mehmet Kaya', 'Izmir'),
  ('Zeynep Sahin', 'Bursa'),
  ('Can Aydin', 'Antalya');

-- ~295 more synthetic users. Names are composed from first/last-name pools.
-- Cities are drawn *proportionally to real population* (TUIK 2023, in thousands)
-- rather than uniformly, so the order stream mirrors reality: Istanbul dominates,
-- the big metros follow, and small provinces contribute only a trickle. Each user
-- draws a random point on the cumulative population range and lands in the city
-- whose [lo, hi) band covers it, giving a population-weighted assignment.
WITH city_w(city, pop) AS (
  VALUES
    ('Istanbul',  15907),
    ('Ankara',     5803),
    ('Izmir',      4479),
    ('Bursa',      3214),
    ('Antalya',    2696),
    ('Konya',      2296),
    ('Adana',      2270),
    ('Gaziantep',  2154),
    ('Mersin',     1917),
    ('Kayseri',    1453),
    ('Samsun',     1379),
    ('Eskisehir',   915)
),
city_cum AS (
  SELECT
    city,
    sum(pop) OVER (ORDER BY pop DESC, city) - pop AS lo,
    sum(pop) OVER (ORDER BY pop DESC, city)       AS hi
  FROM city_w
),
gen AS (
  SELECT i, random() * (SELECT sum(pop) FROM city_w) AS pick
  FROM generate_series(1, 295) AS s(i)
)
INSERT INTO users (full_name, city)
SELECT
  (ARRAY['Ahmet','Elif','Mehmet','Zeynep','Can','Ayse','Mustafa','Fatma',
         'Emre','Selin','Burak','Deniz','Cem','Merve','Ozan','Ece',
         'Kaan','Irem','Baris','Gizem'])[1 + (i % 20)]
  || ' ' ||
  (ARRAY['Yilmaz','Demir','Kaya','Sahin','Aydin','Celik','Arslan','Dogan',
         'Kilic','Ozturk','Aksoy','Korkmaz','Yildiz','Turan','Kara'])[1 + ((i / 20) % 15)],
  (SELECT city FROM city_cum WHERE gen.pick >= lo AND gen.pick < hi LIMIT 1)
FROM gen;

-- The original eight named products (kept from the original seed).
INSERT INTO products (product_id, name, category, price) VALUES
  (1, 'Kablosuz Kulaklik', 'Elektronik', 1299.90),
  (2, 'Bluetooth Hoparlor', 'Elektronik', 899.50),
  (3, 'Kosu Ayakkabisi', 'Ayakkabi', 1599.00),
  (4, 'Sirt Cantasi', 'Aksesuar', 749.90),
  (5, 'Akilli Saat', 'Elektronik', 3499.00),
  (6, 'Pamuklu Tisort', 'Giyim', 299.90),
  (7, 'Termos', 'Aksesuar', 449.00),
  (8, 'Yoga Mati', 'Spor', 399.90);

-- ~42 more products (ids 9..50) spread across the same five categories, with
-- prices varied by an arithmetic spread so order totals and per-item prices have
-- a realistic distribution.
INSERT INTO products (product_id, name, category, price)
SELECT
  i,
  (ARRAY['Elektronik','Ayakkabi','Aksesuar','Giyim','Spor'])[1 + (i % 5)] || ' Urun ' || i,
  (ARRAY['Elektronik','Ayakkabi','Aksesuar','Giyim','Spor'])[1 + (i % 5)],
  round((99 + ((i * 137) % 3900))::numeric, 2)
FROM generate_series(9, 50) AS s(i);

-- Every product starts with a randomised stock level (50..250) so the stock
-- monitor and burn-rate analytics see varied inventory.
INSERT INTO inventory (product_id, stock_qty)
SELECT product_id, 50 + (random() * 200)::int FROM products;
