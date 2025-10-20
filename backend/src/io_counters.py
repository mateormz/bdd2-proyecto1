"""
SELECT * FROM Inventory3D
WHERE x IN (point, [95.367, 33.092, 5]);







# no
SELECT * FROM Inventory3D
WHERE x IN (point, [95.367, 33.092, 10.659, 5]);



SELECT * FROM Inventory3D
WHERE x IN (point, [95.367, 33.092, 10.659]);






# no
SELECT * FROM Inventory3D
WHERE x IN (5, [99.715, 61.926, 10.647]);





CREATE TABLE Inventory3D FROM FILE "../data/warehouse_inventory_test.csv"
USING INDEX RTREE("product_id");
"""