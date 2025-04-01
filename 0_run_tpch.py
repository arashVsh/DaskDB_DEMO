from DaskDB.Context import Context

col_names_lineitem = ['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount'
    ,'l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct'
    ,'l_shipmode', 'l_comment']

col_names_orders = ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority'
                    ,'o_clerk','o_shippriority','o_comment']

col_names_customer = ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']

c = Context()
c.setup_configuration(daskSchedulerIP='localhost', daskSchedulerPort=8786)
c.register_table('orders', 'data/orders.csv', delimiter='|', col_names=col_names_orders)
c.register_table('lineitem', 'data/lineitem.csv', delimiter='|', col_names=col_names_lineitem)
c.register_table('customer', 'data/customer.csv', delimiter='|', col_names=col_names_customer)
c.initSchema()

sql_tpch_1 = """select
    l_orderkey,
    l_partkey,
    sum(l_extendedprice) as sum1,
    avg(l_discount) as avg1,
    sum(l_quantity) as sum2
from
    customer,
    orders,
    lineitem
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
group by
    l_orderkey, l_partkey
order by
    l_partkey
limit 5"""

res = c.query(sql_tpch_1)
print(res)

