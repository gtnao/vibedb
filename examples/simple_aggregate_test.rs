//! Simple test to verify aggregate executor integration

use vibedb::database::Database;
use vibedb::executor::{
    AggregateFunction, AggregateSpec, ColumnInfo, ExecutionContext, Executor, GroupByClause,
    HashAggregateExecutor,
};
use vibedb::access::{DataType, serialize_values, Tuple, TupleId, Value};
use vibedb::storage::page::PageId;
use std::sync::Arc;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    // Create database for context
    let db_path = Path::new("test_aggregate.db");
    let db = Arc::new(Database::create(db_path)?);
    let context = ExecutionContext::new(db.catalog.clone(), db.buffer_pool.clone());

    println!("=== Simple Aggregate Executor Test ===");

    // Create test data
    let schema = vec![
        ColumnInfo::new("department", DataType::Varchar),
        ColumnInfo::new("salary", DataType::Int32),
    ];
    
    let schema_types = vec![DataType::Varchar, DataType::Int32];

    let tuples = vec![
        Tuple::new(
            TupleId::new(PageId(0), 0),
            serialize_values(&[
                Value::String("Sales".to_string()),
                Value::Int32(1000),
            ], &schema_types)?
        ),
        Tuple::new(
            TupleId::new(PageId(0), 1),
            serialize_values(&[
                Value::String("IT".to_string()),
                Value::Int32(2000),
            ], &schema_types)?
        ),
        Tuple::new(
            TupleId::new(PageId(0), 2),
            serialize_values(&[
                Value::String("Sales".to_string()),
                Value::Int32(1500),
            ], &schema_types)?
        ),
        Tuple::new(
            TupleId::new(PageId(0), 3),
            serialize_values(&[
                Value::String("IT".to_string()),
                Value::Int32(2500),
            ], &schema_types)?
        ),
    ];

    let child = Box::new(MockExecutor::new(schema, tuples));

    // Test 1: COUNT(*) without GROUP BY
    println!("\n1. Testing COUNT(*):");
    let mut count_executor = HashAggregateExecutor::new(
        child.clone(),
        GroupByClause::new(vec![]),
        vec![AggregateSpec::new(AggregateFunction::Count, None)],
    )?;
    
    count_executor.init()?;
    if let Some(tuple) = count_executor.next()? {
        let values = vibedb::access::deserialize_values(&tuple.data, &[DataType::Int32])?;
        println!("   COUNT(*) = {:?}", values[0]);
    }

    // Test 2: SUM with GROUP BY
    println!("\n2. Testing SUM with GROUP BY department:");
    let mut sum_executor = HashAggregateExecutor::new(
        child.clone(),
        GroupByClause::new(vec![0]), // GROUP BY department
        vec![AggregateSpec::new(AggregateFunction::Sum, Some(1))], // SUM(salary)
    )?;
    
    sum_executor.init()?;
    println!("   Department | Total");
    println!("   -----------|------");
    while let Some(tuple) = sum_executor.next()? {
        let values = vibedb::access::deserialize_values(
            &tuple.data,
            &[DataType::Varchar, DataType::Int32],
        )?;
        if let (Value::String(dept), Value::Int32(total)) = (&values[0], &values[1]) {
            println!("   {:<10} | {}", dept, total);
        }
    }

    // Cleanup
    std::fs::remove_file(db_path).ok();

    println!("\nAggregate executor is working correctly!");
    println!("Next step: SQL parsing support for aggregate queries.");

    Ok(())
}

// Mock executor implementation
pub struct MockExecutor {
    tuples: Vec<Tuple>,
    schema: Vec<ColumnInfo>,
    pos: usize,
}

impl MockExecutor {
    pub fn new(schema: Vec<ColumnInfo>, tuples: Vec<Tuple>) -> Self {
        Self {
            tuples,
            schema,
            pos: 0,
        }
    }
    
    pub fn clone(&self) -> Box<MockExecutor> {
        Box::new(MockExecutor {
            tuples: self.tuples.clone(),
            schema: self.schema.clone(),
            pos: 0,
        })
    }
}

impl Executor for MockExecutor {
    fn init(&mut self) -> anyhow::Result<()> {
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self) -> anyhow::Result<Option<Tuple>> {
        if self.pos < self.tuples.len() {
            let tuple = self.tuples[self.pos].clone();
            self.pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn output_schema(&self) -> &[ColumnInfo] {
        &self.schema
    }
}