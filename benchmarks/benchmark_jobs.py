import datafruit as dft
import random 
from datetime import date, timedelta

from .tpch_schema import get_tpch_db_instance, LineItem, Orders
from .results_models import get_results_db_instance, QueryMetric

@dft.pyjob(output=LineItem, num_cpus=4)
def generate_lineitem_date(row_number: int):

    start_date = date(2005, 1, 1)
    end_date = date(2011, 12, 31)

    return {
        "l_orderkey": random.randint(1, 6000000),
        "l_partkey": random.randint(1, 200000),
        "l_suppkey": random.randint(1, 10000),
        "l_linenumber": row_number % 4 + 1,
        "l_quantity": round(random.uniform(1.0, 50.0), 2),
        "l_extendedprice": round(random.uniform(100.0, 100000.0), 2),
        "l_discount": round(random.uniform(0.0, 0.1), 2),
        "l_tax": round(random.uniform(0.0, 0.08), 2),
        "l_returnflag": random.choice(['N', 'A', 'R']),
        "l_linestatus": random.choice(['O', 'F']),
        "l_shipdate": start_date + timedelta(days=random_days),
        "l_commitdate": start_date + timedelta(days=random_days + 10),
        "l_receiptdate": start_date + timedelta(days=random_days + 20),
        "l_shipinstruct": random.choice(['DELIVER IN PERSON', 'TAKE BACK RETURN', 'COLLECT']),
        "l_shipmode": random.choice(['AIR', 'MAIL', 'SHIP', 'TRUCK']),
        "l_comment": "xyz comment"
    }

@dft.sql_job()
def run_tpch_query_1(db_instance: dft.PostgresDB):
    """
    Executes TPC-H Query 1 (Pricing Summary Report).
    Returns the results as a Pandas DataFrame for analysis.
    The 'db_instance' is passed dynamically by the orchestrator.
    """
    dft.set_db(db_instance) # Set the target database for this job run
    
    return """
        select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        from
            {{ ref('LineItem') }}
        where
            l_shipdate <= date '1998-12-01' - interval '90' day
        group by
            l_returnflag,
            l_linestatus
        order by
            l_returnflag,
            l_linestatus;
    """


@dft.sql_job()
def run_tpch_query_2(db_instance: dft.PostgresDB):
    """Minimum Cost Supplier Query"""
    dft.set_db(db_instance)

    return """
        select
            s_acctbal,
            s_name,
            n_name,
            p_partkey,
            p_mfgr,
            s_address,
            s_phone,
            s_comment
        from
            {{ ref('Part') }} p,
            {{ ref('Supplier') }} s,
            {{ ref('PartSupp') }} ps,
            {{ ref('Nation') }} n,
            {{ ref('Region') }} r
        where
            p.p_partkey = ps.ps_partkey
            and s.s_suppkey = ps.ps_suppkey
            and p.p_size = 15
            and p.p_type like '%BRASS'
            and s.s_nationkey = n.n_nationkey
            and n.n_regionkey = r.r_regionkey
            and r.r_name = 'EUROPE'
            and ps.ps_supplycost = (
                select
                    min(ps.ps_supplycost)
                from
                    {{ ref('PartSupp') }} ps,
                    {{ ref('Supplier') }} s,
                    {{ ref('Nation') }} n,
                    {{ ref('Region') }} r
                where
                    p.p_partkey = ps.ps_partkey
                    and s.s_suppkey = ps.ps_suppkey
                    and s.s_nationkey = n.n_nationkey
                    and n.n_regionkey = r.r_regionkey
                    and r.r_name = 'EUROPE'
            )
        order by
            s_acctbal desc,
            n_name,
            s_name,
            p_partkey
        limit 100;
    """


@dft.sql_job()
def run_tpch_query_3(db_instance: dft.PostgresDB):
    """Shipping Priority Query"""
    dft.set_db(db_instance)

    return """
        select
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        from
            {{ ref('Customer') }} c,
            {{ ref('Orders') }} o,
            {{ ref('LineItem') }} l
        where
            c.c_mktsegment = 'BUILDING'
            and c.c_custkey = o.o_custkey
            and l.l_orderkey = o.o_orderkey
            and o.o_orderdate < date '1995-03-15'
            and l.l_shipdate > date '1995-03-15'
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate
        limit 10;
    """


@dft.sql_job()
def run_tpch_query_4(db_instance: dft.PostgresDB):
    """Order Priority Checking Query"""
    dft.set_db(db_instance)

    return """
        select
            o_orderpriority,
            count(*) as order_count
        from
            {{ ref('Orders') }} o
        where
            o.o_orderdate >= date '1993-07-01'
            and o.o_orderdate < date '1993-10-01'
            and exists (
                select
                    *
                from
                    {{ ref('LineItem') }} l
                where
                    l.l_orderkey = o.o_orderkey
                    and l.l_commitdate < l.l_receiptdate
            )
        group by
            o_orderpriority
        order by
            o_orderpriority;
    """


@dft.sql_job()
def run_tpch_query_5(db_instance: dft.PostgresDB):
    """Local Supplier Volume Query"""
    dft.set_db(db_instance)

    return """
        select
            n_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue
        from
            {{ ref('Customer') }} c,
            {{ ref('Orders') }} o,
            {{ ref('LineItem') }} l,
            {{ ref('Supplier') }} s,
            {{ ref('Nation') }} n,
            {{ ref('Region') }} r
        where
            c.c_custkey = o.o_custkey
            and l.l_orderkey = o.o_orderkey
            and l.l_suppkey = s.s_suppkey
            and c.c_nationkey = s.s_nationkey
            and s.s_nationkey = n.n_nationkey
            and n.n_regionkey = r.r_regionkey
            and r.r_name = 'ASIA'
            and o.o_orderdate >= date '1994-01-01'
            and o.o_orderdate < date '1995-01-01'
        group by
            n_name
        order by
            revenue desc;
    """


@dft.sql_job()
def run_tpch_query_6(db_instance: dft.PostgresDB):
    """Forecasting Revenue Change Query"""
    dft.set_db(db_instance)

    return """
        select
            sum(l_extendedprice * l_discount) as revenue
        from
            {{ ref('LineItem') }}
        where
            l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1995-01-01'
            and l_discount between 0.05 and 0.07
            and l_quantity < 24;
    """


@dft.sql_job()
def run_tpch_query_7(db_instance: dft.PostgresDB):
    """Volume Shipping Query"""
    dft.set_db(db_instance)

    return """
        select
            supp_nation,
            cust_nation,
            l_year,
            sum(volume) as revenue
        from (
            select
                n1.n_name as supp_nation,
                n2.n_name as cust_nation,
                extract(year from l_shipdate) as l_year,
                l_extendedprice * (1 - l_discount) as volume
            from
                {{ ref('Supplier') }} s,
                {{ ref('LineItem') }} l,
                {{ ref('Orders') }} o,
                {{ ref('Customer') }} c,
                {{ ref('Nation') }} n1,
                {{ ref('Nation') }} n2
            where
                s.s_suppkey = l.l_suppkey
                and o.o_orderkey = l.l_orderkey
                and c.c_custkey = o.o_custkey
                and s.s_nationkey = n1.n_nationkey
                and c.c_nationkey = n2.n_nationkey
                and ((n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                     or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE'))
                and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
        group by
            supp_nation,
            cust_nation,
            l_year
        order by
            supp_nation,
            cust_nation,
            l_year;
    """


@dft.sql_job()
def run_tpch_query_8(db_instance: dft.PostgresDB):
    """National Market Share Query"""
    dft.set_db(db_instance)

    return """
        select
            o_year,
            sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
        from (
            select
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                {{ ref('Part') }} p,
                {{ ref('Supplier') }} s,
                {{ ref('LineItem') }} l,
                {{ ref('Orders') }} o,
                {{ ref('Customer') }} c,
                {{ ref('Nation') }} n1,
                {{ ref('Nation') }} n2,
                {{ ref('Region') }} r
            where
                p.p_partkey = l.l_partkey
                and s.s_suppkey = l.l_suppkey
                and l.l_orderkey = o.o_orderkey
                and o.o_custkey = c.c_custkey
                and c.c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r.r_regionkey
                and r.r_name = 'AMERICA'
                and s.s_nationkey = n2.n_nationkey
                and o.o_orderdate between date '1995-01-01' and date '1996-12-31'
                and p.p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
        group by
            o_year
        order by
            o_year;
    """


@dft.sql_job()
def run_tpch_query_9(db_instance: dft.PostgresDB):
    """Product Type Profit Measure Query"""
    dft.set_db(db_instance)

    return """
        select
            nation,
            o_year,
            sum(amount) as sum_profit
        from (
            select
                n.n_name as nation,
                extract(year from o.o_orderdate) as o_year,
                l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
            from
                {{ ref('Part') }} p,
                {{ ref('Supplier') }} s,
                {{ ref('LineItem') }} l,
                {{ ref('PartSupp') }} ps,
                {{ ref('Orders') }} o,
                {{ ref('Nation') }} n
            where
                s.s_suppkey = l.l_suppkey
                and ps.ps_suppkey = l.l_suppkey
                and ps.ps_partkey = l.l_partkey
                and p.p_partkey = l.l_partkey
                and o.o_orderkey = l.l_orderkey
                and s.s_nationkey = n.n_nationkey
                and p.p_name like '%green%'
        ) as profit
        group by
            nation,
            o_year
        order by
            nation,
            o_year desc;
    """


@dft.sql_job()
def run_tpch_query_10(db_instance: dft.PostgresDB):
    """Returned Item Reporting Query"""
    dft.set_db(db_instance)

    return """
        select
            c_custkey,
            c_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            c_acctbal,
            n_name,
            c_address,
            c_phone,
            c_comment
        from
            {{ ref('Customer') }} c,
            {{ ref('Orders') }} o,
            {{ ref('LineItem') }} l,
            {{ ref('Nation') }} n
        where
            c.c_custkey = o.o_custkey
            and l.l_orderkey = o.o_orderkey
            and o.o_orderdate >= date '1993-10-01'
            and o.o_orderdate < date '1994-01-01'
            and l.l_returnflag = 'R'
            and c.c_nationkey = n.n_nationkey
        group by
            c_custkey,
            c_name,
            c_acctbal,
            c_phone,
            n_name,
            c_address,
            c_comment
        order by
            revenue desc
        limit 20;
    """


@dft.sql_job()
def run_tpch_query_11(db_instance: dft.PostgresDB):
    """Important Stock Identification Query"""
    dft.set_db(db_instance)

    return """
        select
            ps_partkey,
            sum(ps_supplycost * ps_availqty) as value
        from
            {{ ref('PartSupp') }} ps,
            {{ ref('Supplier') }} s,
            {{ ref('Nation') }} n
        where
            ps.ps_suppkey = s.s_suppkey
            and s.s_nationkey = n.n_nationkey
            and n.n_name = 'GERMANY'
        group by
            ps_partkey
        having
            sum(ps_supplycost * ps_availqty) > (
                select
                    sum(ps_supplycost * ps_availqty) * 0.0001
                from
                    {{ ref('PartSupp') }} ps,
                    {{ ref('Supplier') }} s,
                    {{ ref('Nation') }} n
                where
                    ps.ps_suppkey = s.s_suppkey
                    and s.s_nationkey = n.n_nationkey
                    and n.n_name = 'GERMANY'
            )
        order by
            value desc;
    """


@dft.sql_job()
def run_tpch_query_12(db_instance: dft.PostgresDB):
    """Shipping Modes and Type Query"""
    dft.set_db(db_instance)

    return """
        select
            l_shipmode,
            sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
            sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
        from
            {{ ref('Orders') }} o,
            {{ ref('LineItem') }} l
        where
            o.o_orderkey = l.l_orderkey
            and l.l_shipmode in ('MAIL', 'SHIP')
            and l.l_commitdate < l.l_receiptdate
            and l.l_shipdate < l.l_commitdate
            and l.l_receiptdate >= date '1994-01-01'
            and l.l_receiptdate < date '1995-01-01'
        group by
            l_shipmode
        order by
            l_shipmode;
    """


@dft.sql_job()
def run_tpch_query_13(db_instance: dft.PostgresDB):
    """Customer Distribution Query"""
    dft.set_db(db_instance)

    return """
        select
            c_count,
            count(*) as custdist
        from (
            select
                c_custkey,
                count(o_orderkey) as c_count
            from
                {{ ref('Customer') }} c
                left outer join {{ ref('Orders') }} o on c.c_custkey = o.o_custkey
                and o.o_comment not like '%special%requests%'
            group by
                c_custkey
        ) as c_orders
        group by
            c_count
        order by
            custdist desc,
            c_count desc;
    """


@dft.sql_job()
def run_tpch_query_14(db_instance: dft.PostgresDB):
    """Promotion Effect Query"""
    dft.set_db(db_instance)

    return """
        select
            100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) /
            sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        from
            {{ ref('LineItem') }} l,
            {{ ref('Part') }} p
        where
            l.l_partkey = p.p_partkey
            and l.l_shipdate >= date '1994-03-01'
            and l.l_shipdate < date '1994-04-01';
    """


@dft.sql_job()
def run_tpch_query_15(db_instance: dft.PostgresDB):
    """Top Supplier Query"""
    dft.set_db(db_instance)

    return """
        create view revenue0 (supplier_no, total_revenue) as
        select
            l_suppkey,
            sum(l_extendedprice * (1 - l_discount))
        from
            {{ ref('LineItem') }}
        where
            l_shipdate >= date '1996-01-01'
            and l_shipdate < date '1996-04-01'
        group by
            l_suppkey;

        select
            s_suppkey,
            s_name,
            s_address,
            s_phone,
            total_revenue
        from
            {{ ref('Supplier') }} s,
            revenue0
        where
            s.s_suppkey = supplier_no
            and total_revenue = (
                select
                    max(total_revenue)
                from
                    revenue0
            )
        order by
            s_suppkey;

        drop view revenue0;
    """


@dft.sql_job()
def run_tpch_query_16(db_instance: dft.PostgresDB):
    """Parts/Supplier Relationship Query"""
    dft.set_db(db_instance)

    return """
        select
            p_brand,
            p_type,
            p_size,
            count(distinct ps_suppkey) as supplier_cnt
        from
            {{ ref('PartSupp') }} ps,
            {{ ref('Part') }} p
        where
            p.p_partkey = ps.ps_partkey
            and p.p_brand <> 'Brand#45'
            and p.p_type not like 'MEDIUM POLISHED%'
            and p.p_size in (49, 14, 23, 45, 19, 3, 36, 9)
            and ps.ps_suppkey not in (
                select
                    s_suppkey
                from
                    {{ ref('Supplier') }}
                where
                    s_comment like '%Customer%Complaints%'
            )
        group by
            p_brand,
            p_type,
            p_size
        order by
            supplier_cnt desc,
            p_brand,
            p_type,
            p_size;
    """


@dft.sql_job()
def run_tpch_query_17(db_instance: dft.PostgresDB):
    """Small-Quantity-Order Revenue Query"""
    dft.set_db(db_instance)

    return """
        select
            sum(l_extendedprice) / 7.0 as avg_yearly
        from
            {{ ref('LineItem') }} l,
            {{ ref('Part') }} p
        where
            p.p_partkey = l.l_partkey
            and p.p_brand = 'Brand#23'
            and p.p_container = 'MED BOX'
            and l.l_quantity < (
                select
                    0.2 * avg(l_quantity)
                from
                    {{ ref('LineItem') }}
                where
                    l_partkey = p.p_partkey
            );
    """


@dft.sql_job()
def run_tpch_query_18(db_instance: dft.PostgresDB):
    """Large Volume Customer Query"""
    dft.set_db(db_instance)

    return """
        select
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice,
            sum(l_quantity)
        from
            {{ ref('Customer') }} c,
            {{ ref('Orders') }} o,
            {{ ref('LineItem') }} l
        where
            o.o_orderkey in (
                select
                    l_orderkey
                from
                    {{ ref('LineItem') }}
                group by
                    l_orderkey
                having
                    sum(l_quantity) > 300
            )
            and c.c_custkey = o.o_custkey
            and o.o_orderkey = l.l_orderkey
        group by
            c_name,
            c_custkey,
            o_orderkey,
            o_orderdate,
            o_totalprice
        order by
            o_totalprice desc,
            o_orderdate
        limit 100;
    """


@dft.sql_job()
def run_tpch_query_19(db_instance: dft.PostgresDB):
    """Discounted Revenue Query"""
    dft.set_db(db_instance)

    return """
        select
            sum(l_extendedprice * (1 - l_discount)) as revenue
        from
            {{ ref('LineItem') }} l,
            {{ ref('Part') }} p
        where
            (
                p.p_partkey = l.l_partkey
                and p.p_brand = 'Brand#12'
                and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l.l_quantity >= 1 and l.l_quantity <= 11
                and p.p_size between 1 and 5
                and l.l_shipmode in ('AIR', 'AIR REG')
                and l.l_shipinstruct = 'DELIVER IN PERSON'
            )
            or
            (
                p.p_partkey = l.l_partkey
                and p.p_brand = 'Brand#23'
                and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l.l_quantity >= 10 and l.l_quantity <= 20
                and p.p_size between 1 and 10
                and l.l_shipmode in ('AIR', 'AIR REG')
                and l.l_shipinstruct = 'DELIVER IN PERSON'
            )
            or
            (
                p.p_partkey = l.l_partkey
                and p.p_brand = 'Brand#34'
                and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l.l_quantity >= 20 and l.l_quantity <= 30
                and p.p_size between 1 and 15
                and l.l_shipmode in ('AIR', 'AIR REG')
                and l.l_shipinstruct = 'DELIVER IN PERSON'
            );
    """


@dft.sql_job()
def run_tpch_query_20(db_instance: dft.PostgresDB):
    """Potential Part Promotion Query"""
    dft.set_db(db_instance)

    return """
        select
            s_name,
            s_address
        from
            {{ ref('Supplier') }} s,
            {{ ref('Nation') }} n
        where
            s.s_suppkey in (
                select
                    ps_suppkey
                from
                    {{ ref('PartSupp') }} ps
                where
                    ps.ps_partkey in (
                        select
                            p_partkey
                        from
                            {{ ref('Part') }}
                        where
                            p_name like 'forest%'
                    )
                    and ps.ps_availqty > (
                        select
                            0.5 * sum(l_quantity)
                        from
                            {{ ref('LineItem') }}
                        where
                            l_partkey = ps.ps_partkey
                            and l_suppkey = ps.ps_suppkey
                            and l_shipdate >= date '1994-01-01'
                            and l_shipdate < date '1995-01-01'
                    )
            )
            and s.s_nationkey = n.n_nationkey
            and n.n_name = 'CANADA'
        order by
            s_name;
    """


@dft.sql_job()
def run_tpch_query_21(db_instance: dft.PostgresDB):
    """Suppliers Who Kept Orders Waiting Query"""
    dft.set_db(db_instance)

    return """
        select
            s_name,
            count(*) as numwait
        from
            {{ ref('Supplier') }} s,
            {{ ref('LineItem') }} l1,
            {{ ref('Orders') }} o,
            {{ ref('Nation') }} n
        where
            s.s_suppkey = l1.l_suppkey
            and o.o_orderkey = l1.l_orderkey
            and o.o_orderstatus = 'F'
            and l1.l_receiptdate > l1.l_commitdate
            and exists (
                select
                    *
                from
                    {{ ref('LineItem') }} l2
                where
                    l2.l_orderkey = l1.l_orderkey
                    and l2.l_suppkey <> l1.l_suppkey
            )
            and not exists (
                select
                    *
                from
                    {{ ref('LineItem') }} l3
                where
                    l3.l_orderkey = l1.l_orderkey
                    and l3.l_suppkey <> l1.l_suppkey
                    and l3.l_receiptdate > l3.l_commitdate
            )
            and s.s_nationkey = n.n_nationkey
            and n.n_name = 'SAUDI ARABIA'
        group by
            s_name
        order by
            numwait desc,
            s_name
        limit 100;
    """


@dft.sql_job()
def run_tpch_query_22(db_instance: dft.PostgresDB):
    """Global Sales Opportunity Query"""
    dft.set_db(db_instance)

    return """
        select
            cntrycode,
            count(*) as numcust,
            sum(c_acctbal) as totacctbal
        from (
            select
                substring(c_phone from 1 for 2) as cntrycode,
                c_acctbal
            from
                {{ ref('Customer') }}
            where
                substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
                and c_acctbal > (
                    select
                        avg(c_acctbal)
                    from
                        {{ ref('Customer') }}
                    where
                        c_acctbal > 0.00
                        and substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
                )
                and not exists (
                    select
                        *
                    from
                        {{ ref('Orders') }}
                    where
                        o_custkey = c_custkey
                )
        ) as custsale
        group by
            cntrycode
        order by
            cntrycode;
    """