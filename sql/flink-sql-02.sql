CREATE TABLE products (
    id INT,
    name STRING,
    description STRING,
    PRIMARY KEY (id) NOT ENFORCED
  ) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.11.0.142',
    'port' = '3306',
    'username' = 'root',
    'password' = '123456',
    'database-name' = 'mydb',
    'table-name' = 'products'
  );
CREATE TABLE orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
   'connector' = 'mysql-cdc',
   'hostname' = '10.11.0.142',
   'port' = '3306',
   'username' = 'root',
   'password' = '123456',
   'database-name' = 'mydb',
   'table-name' = 'orders'
 );
CREATE TABLE shipments (
   shipment_id INT,
   order_id INT,
   origin STRING,
   destination STRING,
   is_arrived BOOLEAN,
   PRIMARY KEY (shipment_id) NOT ENFORCED
 ) WITH (
   'connector' = 'mysql-cdc',
   'hostname' = '10.11.0.142',
   'port' = '3306',
   'username' = 'root',
   'password' = '123456',
   'database-name' = 'mydb',
   'table-name' = 'shipments'
 );
CREATE TABLE enriched_orders (
   order_id INT,
   order_date TIMESTAMP(0),
   customer_name STRING,
   price DECIMAL(10, 5),
   product_id INT,
   order_status BOOLEAN,
   product_name STRING,
   product_description STRING,
   shipment_id INT,
   origin STRING,
   destination STRING,
   is_arrived BOOLEAN,
   PRIMARY KEY (order_id) NOT ENFORCED
 ) WITH (
     'connector' = 'elasticsearch-7',
     'hosts' = 'http://10.11.0.142:9200',
     'index' = 'enriched_orders'
 );
INSERT INTO enriched_orders
 SELECT o.*, p.name, p.description, s.shipment_id, s.origin, s.destination, s.is_arrived
 FROM orders AS o
 LEFT JOIN products AS p ON o.product_id = p.id
 LEFT JOIN shipments AS s ON o.order_id = s.order_id;