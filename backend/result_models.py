from sqlmodel import Field, SQLModel, Relationship
from typing import List, Optional
from datetime import datetime
import uuid

class BenchmarkRun(SQLModel, table = True):
    job_id: uuid.UUID = Field(default=uuid.uuid4, primary_key = True, description = "ID for benchmark job")

    db_type: str = Field(index = True, description = "Type of DB benchmarked")
    scale_factor: int = Field(description = "TPC-H scale factor")
    status: str = Field(index = True, default = "pending", description = "current status of job")

    # specific metrics from HammerDB
    power_score: Optional[float] = Field(description = "gemoetric mean of the query times")
    throughput_score: Optional[float] = Field(description = "queries per hour")

    created_at: datetime = Field(default_factory=datetime.utcnow, description="when the job was created")
    completed_at: Optional[datetime] = Field(default=None, description="when the job finished")

    # backfills relations for individual queries
    query_metrics: List["QueryMetric"] = Relationship(back_populates="benchmark_run")

class QueryMetric(SQLModel, table=True):   
    metric_id: Optional[int] = Field(default=None, primary_key=True)
    
    job_id: uuid.UUID = Field(foreign_key="benchmarkrun.job_id", description="The job this metric belongs to")
    query_number: int = Field(description="The TPC-H query number (1-22)")
    execution_time_seconds: float = Field(description="Time taken to execute the query in seconds")

    # backfill relationship back to the parent run
    benchmark_run: Optional[BenchmarkRun] = Relationship(back_populates="query_metrics")

RESULTS_DB_MODELS = [
    BenchmarkRun,
    QueryMetric,
]

def get_results_db_instance() -> dft.PostgresDB:
    """
    Creates a datafruit.PostgresDB instance for the service
    results database. It reads the connection string from an environment
    variable.
    """

    connection_string = os.getenv("RESULTS_DB_URL")
    if not connection_string:
        raise ValueError("RESULTS_DB_URL environment variable not set.")
        
    db = dft.PostgresDB(
        connection_string=connection_string,
        tables=RESULTS_DB_MODELS
    )
    return db
