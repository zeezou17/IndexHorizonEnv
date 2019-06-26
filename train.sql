-- Query 1 : ##########################################################################################################
select l_returnflag,
       l_linestatus,
       sum(l_quantity)                                       as sum_qty,
       sum(l_extendedprice)                                  as sum_base_price,
       sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity)                                       as avg_qty,
       avg(l_extendedprice)                                  as avg_price,
       avg(l_discount)                                       as avg_disc,
       count(*)                                              as count_order
from lineitem
where l_shipdate <= date '1998-12-01' - interval '71 days'
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus;

-- Query 3: ##########################################################################################################
select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
from customer,
     orders,
     lineitem
where c_mktsegment = 'FURNITURE'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-29'
  and l_shipdate > date '1995-03-29'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate
LIMIT 10;

-- Query 5 : ##########################################################################################################
select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
from customer,
     orders,
     lineitem,
     supplier,
     nation,
     region
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'MIDDLE EAST'
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '1' year
group by n_name
order by revenue desc;

-- Query 10 : ##########################################################################################################
select c_custkey,
       c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
from customer,
     orders,
     lineitem,
     nation
where c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1994-01-01'
  and o_orderdate < date '1994-01-01' + interval '3' month
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
order by revenue desc
LIMIT 20;

-- Query 14 : ##########################################################################################################
select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) /
       sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1994-03-01'
  and l_shipdate < date '1994-03-01' + interval '1' month;