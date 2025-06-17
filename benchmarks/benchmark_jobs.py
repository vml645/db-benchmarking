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