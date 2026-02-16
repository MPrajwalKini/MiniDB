"""
MiniDB Executor
===============
End-to-end SQL Execution Engine.
Pipeline: SQL -> Parser -> Logical Planner -> Physical Planner -> Execution.
"""

from typing import Iterator, Any, Dict, List, Optional
import traceback

from parser import parse
from parser.ast_nodes import Statement
from planning.planner import Planner
from execution.planner import PhysicalPlanner
from execution.context import ExecutionContext
from execution.physical_plan import ExecutionRow

class Executor:
    """
    Executes SQL queries.
    """
    def __init__(self, context: ExecutionContext):
        self.context = context
        self.logical_planner = Planner(context.catalog)
        self.physical_planner = PhysicalPlanner(context)

    def execute(self, sql: str) -> Iterator[ExecutionRow]:
        """
        Execute SQL query and yield result rows.
        """
        # 1. Parse
        ast = parse(sql)
        
        # 2. Logical Plan
        logical_plan = self.logical_planner.plan(ast)
        
        # 3. Physical Plan
        physical_plan = self.physical_planner.plan(logical_plan)
        
        # 4. Execute
        # Resource Guarantee: Ensure close() is called.
        try:
            physical_plan.open()
            while True:
                row = physical_plan.next()
                if row is None:
                    break
                yield row
        finally:
            physical_plan.close()
            
    def execute_and_fetchall(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute and return list of dicts (values only).
        Helper for tests/API.
        """
        results = []
        for row in self.execute(sql):
            results.append(row.values)
        return results
