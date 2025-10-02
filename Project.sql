--1
select * from Customers where Country='Germany'
--2
select ProductName , UnitPrice from Products order by UnitPrice desc

--3
select OrderID, OrderDate,CompanyName from Orders o
join Customers c on o.CustomerID=c.CustomerID
--4
select ProductName, UnitsInStock, UnitPrice from Products 
where UnitsInStock <=10 and Discontinued=0


--5
select o.CustomerID,SUM((Quantity * UnitPrice * (1 - Discount))) [sum of value]
from [Order Details] od
join Orders o on od.OrderID=o.OrderID
group by o.CustomerID
order by [sum of value] desc

SELECT CustomerID, CompanyName
FROM Customers
WHERE CustomerID IN (
    SELECT CustomerID
    FROM Orders
    WHERE YEAR(OrderDate) = 1998)

SELECT DISTINCT c.CustomerID, c.CompanyName
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE YEAR(OrderDate) = 1998

select c.CustomerID , CompanyName,COUNT(o.OrderID)[order count] from Customers c
join Orders o on c.CustomerID=o.CustomerID
where YEAR(OrderDate)=1997
group by c.CustomerID , CompanyName
having COUNT(o.OrderID)> 5
order by [order count] desc

SELECT 
    p.ProductName, 
    p.CategoryID, 
    p.UnitPrice, 
    RANK() OVER (PARTITION BY p.CategoryID ORDER BY p.UnitPrice DESC) AS Rank
FROM Products p
ORDER BY p.CategoryID, Rank


select E.EmployeeID, FirstName , LastName, sum(( Quantity * UnitPrice * (1 - Discount))) [order of value] 
from Employees E
join Orders O on E.EmployeeID =O.EmployeeID
INNER JOIN [Order Details] od ON o.OrderID = od.OrderID
WHERE YEAR(o.OrderDate) = 1998
group by E.EmployeeID, FirstName, LastName
having sum(( Quantity * UnitPrice * (1 - Discount))) >50000
order by [order of value] desc

SELECT 
    p.ProductName, 
    p.CategoryID, 
    p.UnitPrice, 
    AVG(p.UnitPrice) OVER (PARTITION BY p.CategoryID) AS AvgPrice
FROM Products p
WHERE p.UnitPrice > AVG(p.UnitPrice) OVER (PARTITION BY p.CategoryID)
ORDER BY p.CategoryID, p.UnitPrice;

--
SELECT 
    p.ProductName, 
    p.CategoryID, 
    p.UnitPrice, 
    ca.AvgPrice
FROM Products p
INNER JOIN (
    SELECT CategoryID, AVG(UnitPrice) AS AvgPrice
    FROM Products
    GROUP BY CategoryID
) ca ON p.CategoryID = ca.CategoryID
WHERE p.UnitPrice > ca.AvgPrice
ORDER BY p.CategoryID, p.UnitPrice;


select CategoryID,AVG(UnitPrice) [avg_u] from Products group by CategoryID

select p.ProductName, 
    p.CategoryID, 
    p.UnitPrice from [Products] p
inner join (select CategoryID,AVG(UnitPrice) [avg_u] from Products group by CategoryID) ca
 on ca.CategoryID=p.CategoryID
 WHERE p.UnitPrice > ca.[avg_u]
 ORDER BY p.CategoryID, p.UnitPrice

----------------------------------
--????? - ??????? ?? ?????? ?? ? ???? ????
select ProductName, UnitsInStock,UnitPrice from Products
where UnitsInStock<20 and UnitPrice<50
order by UnitPrice desc

--??????? ?? ??????? ???
select * from Orders
select CustomerID , CompanyName from Customers
where CustomerID IN (select CustomerID from Orders where Freight>100 and YEAR(OrderDate)=1998)

--??????? ????????
SELECT 
    p.ProductID, 
    p.ProductName, 
    SUM(od.Quantity) AS TotalOrdered
FROM Products p, [Order Details] od
WHERE p.ProductID = od.ProductID
GROUP BY p.ProductID, p.ProductName
HAVING SUM(od.Quantity) > 500
ORDER BY TotalOrdered DESC;

--??????? ???? ????? ?? ???? ?????
select CustomerID , CompanyName from Customers
where CustomerID 
NOT IN(select CustomerID from Orders where YEAR(OrderDate)=1997)


--??????? ?? ????? ?????? ?? ???? ??
select ProductName , UnitsInStock , UnitsOnOrder from Products
where UnitsOnOrder>UnitsInStock
ORDER BY (UnitsOnOrder - UnitsInStock) DESC


--??????? ?? ?????? ??????
select ProductName, UnitsInStock, ReorderLevel from Products
where UnitsInStock < ReorderLevel and Discontinued=0
order by ReorderLevel - UnitsInStock desc


--??????? ???? ?? ????? ?????

select CustomerID , CompanyName from Customers where CustomerID IN (
select CustomerID from Orders where OrderDate Between 1996 and 1998
group by  CustomerID 
having COUNT(CustomerID) >=3)

--????????? ??????? ?? ???? ??????
select ProductName, UnitsInStock , Rank() over(order by unitsinstock) [Ranking]
from Products
order by [Ranking] desc

-- ??????? ???? ?????? ????
select CustomerID , CompanyName from Customers
where CustomerID Not IN (select CustomerID from Orders 
WHERE OrderDate >= '1998-01-01')


--??????? ?? ???? ???? ?? ???? ?????
SELECT p.ProductID , p.ProductName, SUM(od.Quantity)  [TotalSold]
FROM Products p, [Order Details] od
WHERE p.ProductID = od.ProductID
AND od.OrderID IN ( SELECT OrderID FROM Orders WHERE YEAR(OrderDate) BETWEEN 1997 AND 1998)
GROUP BY p.ProductID, p.ProductName
HAVING SUM(od.Quantity) > 1000
ORDER BY TotalSold DESC;

--??????? ?? ??????? ??? ?? ??? ???
select CustomerID,CompanyName from Customers
where CustomerID IN (
select CustomerID from Orders where YEAR(OrderDate)=1997 and MONTH(OrderDate)=6)


-- CTE
--????????? ??????? ?? ???? ????? ???????

WITH CustomerOrders AS (SELECT CustomerID, CompanyName, 
(SELECT COUNT(OrderID) FROM Orders o WHERE o.CustomerID = c.CustomerID) AS OrderCount
FROM Customers c)

select CustomerID, CompanyName, OrderCount , rank()over(order by OrderCount desc)[ranking]
from CustomerOrders
order by ranking

--??????? ?? ??????? ???? ?? ??? ???
with CustomerOrders AS (SELECT CustomerID, CompanyName,
(select count(OrderID) from Orders o where o.CustomerID =c.CustomerID and YEAR(OrderDate)=1997)[ORDER_COUNT]
from Customers c)

select CustomerID, CompanyName,ORDER_COUNT from CustomerOrders
where ORDER_COUNT>5
order by ORDER_COUNT desc


--??????? ?? ?????? ?????? ? ?????????

with Row_stock_products as(select ProductName ,UnitsInStock,ReorderLevel from Products p
where UnitsInStock<ReorderLevel)

select ProductName ,UnitsInStock,ReorderLevel,rank()over(order by UnitsInStock desc) [Ranking] from Row_stock_products
order by Ranking


--??????? ???? ???? ????

WITH NoSaleProducts AS (
    SELECT 
        ProductID, 
        ProductName, 
        UnitsInStock
    FROM Products
    WHERE ProductID NOT IN (
        SELECT ProductID
        FROM [Order Details]
        WHERE OrderID IN (
            SELECT OrderID
            FROM Orders
            WHERE YEAR(OrderDate) = 1998
        )
    )
)
select  ProductID, 
        ProductName, 
        UnitsInStock from NoSaleProducts
		where UnitsInStock>0
		ORDER BY UnitsInStock DESC



--???? ???? ?? ?????
WITH product_sales AS (
    SELECT 
        p.ProductID,
        ProductName,
        (
            SELECT SUM(Quantity) 
            FROM [Order Details] od 
            WHERE p.ProductID = od.ProductID
            AND OrderID IN (
                SELECT OrderID 
                FROM Orders 
                WHERE YEAR(OrderDate) = 1997
            )
        ) [sum of quantity sales in 1997]
    FROM Products p
),
PercentageCalc AS (
    SELECT 
        ProductID,
        ProductName,
        [sum of quantity sales in 1997],
        ([sum of quantity sales in 1997] * 100.0 / SUM([sum of quantity sales in 1997]) OVER ()) [Percentage]
    FROM product_sales
    WHERE [sum of quantity sales in 1997] IS NOT NULL
)
SELECT 
    ProductID,
    ProductName,
    [sum of quantity sales in 1997],
    [Percentage]
FROM PercentageCalc
WHERE [Percentage] > 2
ORDER BY [Percentage] DESC;