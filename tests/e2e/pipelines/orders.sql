INSERT INTO {{ sink('out', 'orders', format: 'csv') }}
select * from {{ source('wh', 'orders') }}
