

select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders odr,
	lineitem
where
	c_mktsegment = 'MACHINERY'
	and c_custkey = odr.o_custkey
	and l_orderkey = odr.o_orderkey
	and odr.o_orderdate < date '1995-03-30'
	and l_shipdate > date '1995-03-30'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;

select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) /
       sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from lineitem,
     part
where l_partkey = p_partkey
  and l_shipdate >= date '1994-03-01'
  and l_shipdate < date '1994-03-01' + interval '1' month;
