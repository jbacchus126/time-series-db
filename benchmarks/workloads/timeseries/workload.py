"""
Timeseries Workload Plugin
Registers custom runners and parameter sources for M3QL queries
"""


def register(registry):
    """
    Register custom runners and parameter sources with OpenSearch Benchmark.
    This function is called automatically by OSB when loading the workload.
    
    Args:
        registry: WorkloadPluginReader instance with registration methods
    """
    # Import our custom runner module
    from . import m3ql_bulk_runner
    
    # Call register function
    m3ql_bulk_runner.register(registry)
