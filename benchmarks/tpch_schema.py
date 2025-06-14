
import datafruit as dft
from sqlmodel import field, SQLModel
from typing import Optional
from datetime import date

# This file defines the TPC-H benchmark schema
# and a factory function to create a datafruit.PostgresDB instance for it.

class Nation(SQLModel, table = True):
    __tablename__ = 'nation'
    n_nationkey: int = Field(primary_key=true, description = "Nation Key")
    n_name: str = Field(max_length=25, description = "Nation Name")
    n_regionkey: int = Field(foreign_key="region.r_regionkey", description  = "Region Key")
    n_comment: Optional[str] = Field(max_length = 152, description = "Comment")

class Region(SQLModel, table = True):
    __tablename__ = 'region'
    r_regionkey: int = Field(primary_key=true, description = "Region Key")
    r_name: str = Field(max_length=25, description = "Region Name")
    r_comment: Optional[str] = Field(max_length = 152, description = "Comment")

class Part(SQLModel, table=True):
    __tablename__ = 'part'
    p_partkey: int = Field(primary_key=True, description="Part Key")
    p_name: str = Field(max_length=55, description="Part Name")
    p_mfgr: str = Field(max_length=25, description="Manufacturer")
    p_brand: str = Field(max_length=10, description="Brand")
    p_type: str = Field(max_length=25, description="Type")
    p_size: int = Field(description="Size")
    p_container: str = Field(max_length=10, description="Container")
    p_retailprice: float = Field(description="Retail Price")
    p_comment: Optional[str] = Field(max_length=23, description="Comment")

class Supplier(SQLModel, table=True):
    __tablename__ = 'supplier'
    s_suppkey: int = Field(primary_key=True, description="Supplier Key")
    s_name: str = Field(max_length=25, description="Supplier Name")
    s_address: str = Field(max_length=40, description="Address")
    s_nationkey: int = Field(foreign_key="nation.n_nationkey", description="Nation Key")
    s_phone: str = Field(max_length=15, description="Phone Number")
    s_acctbal: float = Field(description="Account Balance")
    s_comment: Optional[str] = Field(max_length=101, description="Comment")

class PartSupp(SQLModel, table=True):
    __tablename__ = 'partsupp'
    ps_partkey: int = Field(primary_key=True, foreign_key="part.p_partkey", description="Part Key")
    ps_suppkey: int = Field(primary_key=True, foreign_key="supplier.s_suppkey", description="Supplier Key")
    ps_availqty: int = Field(description="Available Quantity")
    ps_supplycost: float = Field(description="Supply Cost")
    ps_comment: Optional[str] = Field(max_length=199, description="Comment")

class Customer(SQLModel, table=True):
    __tablename__ = 'customer'
    c_custkey: int = Field(primary_key=True, description="Customer Key")
    c_name: str = Field(max_length=25, description="Customer Name")
    c_address: str = Field(max_length=40, description="Address")
    c_nationkey: int = Field(foreign_key="nation.n_nationkey", description="Nation Key")
    c_phone: str = Field(max_length=15, description="Phone Number")
    c_acctbal: float = Field(description="Account Balance")
    c_mktsegment: str = Field(max_length=10, description="Market Segment")
    c_comment: Optional[str] = Field(max_length=117, description="Comment")

class Orders(SQLModel, table=True):
    __tablename__ = 'orders'
    o_orderkey: int = Field(primary_key=True, description="Order Key")
    o_custkey: int = Field(foreign_key="customer.c_custkey", description="Customer Key")
    o_orderstatus: str = Field(max_length=1, description="Order Status")
    o_totalprice: float = Field(description="Total Price")
    o_orderdate: date = Field(description="Order Date")
    o_orderpriority: str = Field(max_length=15, description="Order Priority")
    o_clerk: str = Field(max_length=15, description="Clerk")
    o_shippriority: int = Field(description="Ship Priority")
    o_comment: Optional[str] = Field(max_length=79, description="Comment")

class LineItem(SQLModel, table=True):
    __tablename__ = 'lineitem'
    l_orderkey: int = Field(primary_key=True, foreign_key="orders.o_orderkey", description="Order Key")
    l_partkey: int = Field(foreign_key="part.p_partkey", description="Part Key")
    l_suppkey: int = Field(foreign_key="supplier.s_suppkey", description="Supplier Key")
    l_linenumber: int = Field(primary_key=True, description="Line Number")
    l_quantity: float = Field(description="Quantity")
    l_extendedprice: float = Field(description="Extended Price")
    l_discount: float = Field(description="Discount")
    l_tax: float = Field(description="Tax")
    l_returnflag: str = Field(max_length=1, description="Return Flag")
    l_linestatus: str = Field(max_length=1, description="Line Status")
    l_shipdate: date = Field(description="Ship Date")
    l_commitdate: date = Field(description="Commit Date")
    l_receiptdate: date = Field(description="Receipt Date")
    l_shipinstruct: str = Field(max_length=25, description="Shipping Instructions")
    l_shipmode: str = Field(max_length=10, description="Shipping Mode")
    l_comment: Optional[str] = Field(max_length=44, description="Comment")

TPCH_MODELS = [
    Region,
    Nation,
    Part,
    Supplier,
    PartSupp,
    Customer,
    Orders,
    LineItem,
]

def get_tpch_db_instance(connection_string: str) -> dft.PostgresDB:
    """
    Creates a datafruit.PostgresDB instance for a given
    connection string, configured with the complete TPC-H schema.

    """
    db = dft.PostgresDB(
        connection_string=connection_string,
        tables=TPCH_MODELS
    )
    return db