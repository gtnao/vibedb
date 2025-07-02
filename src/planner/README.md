# Query Planner

The query planner is responsible for converting SQL AST into executable query plans. It performs a two-stage transformation:

1. **SQL AST → Logical Plan**: Creates a high-level, optimizer-friendly representation
2. **Logical Plan → Physical Plan**: Chooses concrete algorithms and execution strategies

## Architecture

### Logical Plans

Logical plans represent the "what" of query execution without specifying "how". They include:

- `TableScan`: Read from a table
- `Filter`: Apply predicates
- `Projection`: Select specific columns
- `Sort`: Order results
- `Limit`: Restrict result count
- `Join`: Combine relations
- `GroupBy`: Aggregate data
- `Having`: Filter aggregated results
- `Distinct`: Remove duplicates

### Physical Plans

Physical plans specify exact execution algorithms:

- `SeqScan`: Sequential table scan
- `IndexScan`: Index-based access (future)
- `NestedLoopJoin`: Simple join algorithm
- `HashJoin`: Hash-based join (future)
- `HashAggregate`: Hash-based grouping
- `Sort`: In-memory sorting

## Usage

```rust
use vibedb::planner::Planner;
use vibedb::sql::ast::Statement;

// Create planner with catalog
let planner = Planner::new(catalog);

// Convert SQL AST to physical plan
let physical_plan = planner.plan(sql_statement)?;

// Execute the plan (future integration)
// let executor = create_executor(physical_plan, context)?;
// while let Some(tuple) = executor.next()? {
//     // Process results
// }
```

## Current Limitations

- No cost-based optimization
- Always uses sequential scans (no index support yet)
- Always uses nested loop joins (no hash join yet)
- No statistics collection
- SELECT * not implemented (requires schema expansion)

## Future Enhancements

1. **Cost-Based Optimization**
   - Collect table statistics
   - Estimate selectivity
   - Choose optimal join order
   - Select between algorithms (e.g., hash vs nested loop join)

2. **Rule-Based Optimization**
   - Predicate pushdown
   - Projection pushdown
   - Join reordering
   - Common subexpression elimination

3. **Index Support**
   - Recognize index opportunities
   - Choose between seq scan and index scan
   - Support index-only scans

4. **Advanced Features**
   - Subquery optimization
   - View expansion
   - CTE (Common Table Expression) support
   - Window functions

## Integration with Executors

The physical plan nodes map directly to executor implementations:

| Physical Plan Node | Executor Implementation |
|-------------------|------------------------|
| SeqScan | SeqScanExecutor |
| Filter | FilterExecutor |
| Projection | ProjectionExecutor |
| Sort | SortExecutor |
| Limit | LimitExecutor |
| NestedLoopJoin | NestedLoopJoinExecutor |
| HashAggregate | HashAggregateExecutor |

The planner ensures that the physical plan respects the capabilities of the available executors.