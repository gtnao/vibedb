//! Example demonstrating the expression evaluation framework

use vibedb::access::{DataType, Value};
use vibedb::expression::{
    evaluate_expression, expression_to_predicate, Expression, ExpressionEvaluator, TypeChecker,
};

fn main() -> anyhow::Result<()> {
    println!("Expression Evaluation Framework Demo");
    println!("===================================");

    // Example 1: Basic expression evaluation
    println!("\n1. Basic Expression Evaluation");
    println!("------------------------------");

    // Create sample tuple values
    let tuple_values = vec![
        Value::Int32(25),              // age
        Value::String("Alice".into()), // name
        Value::Boolean(true),          // active
    ];

    // Schema for type checking
    let schema = vec![DataType::Int32, DataType::Varchar, DataType::Boolean];

    // age > 18
    let expr1 = Expression::gt(Expression::column(0), Expression::literal(Value::Int32(18)));
    let result1 = evaluate_expression(&expr1, &tuple_values)?;
    println!("age > 18: {:?}", result1);

    // name = "Alice"
    let expr2 = Expression::eq(
        Expression::column(1),
        Expression::literal(Value::String("Alice".into())),
    );
    let result2 = evaluate_expression(&expr2, &tuple_values)?;
    println!("name = 'Alice': {:?}", result2);

    // Example 2: Complex expressions
    println!("\n2. Complex Expression Evaluation");
    println!("--------------------------------");

    // (age > 20) AND active
    let expr3 = Expression::and(
        Expression::gt(Expression::column(0), Expression::literal(Value::Int32(20))),
        Expression::column(2),
    );
    let result3 = evaluate_expression(&expr3, &tuple_values)?;
    println!("(age > 20) AND active: {:?}", result3);

    // age + 5
    let expr4 = Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(5)));
    let result4 = evaluate_expression(&expr4, &tuple_values)?;
    println!("age + 5: {:?}", result4);

    // Example 3: NULL handling
    println!("\n3. NULL Handling");
    println!("----------------");

    let tuple_with_null = vec![
        Value::Null,
        Value::String("Bob".into()),
        Value::Boolean(false),
    ];

    // IS NULL check
    let expr5 = Expression::is_null(Expression::column(0));
    let result5 = evaluate_expression(&expr5, &tuple_with_null)?;
    println!("age IS NULL: {:?}", result5);

    // NULL propagation in arithmetic
    let expr6 = Expression::add_expr(Expression::column(0), Expression::literal(Value::Int32(10)));
    let result6 = evaluate_expression(&expr6, &tuple_with_null)?;
    println!("NULL + 10: {:?}", result6);

    // Example 4: Type checking
    println!("\n4. Type Checking");
    println!("----------------");

    let type_checker = TypeChecker::new(&schema);

    // Valid expression
    let valid_expr = Expression::gt(Expression::column(0), Expression::literal(Value::Int32(30)));
    match type_checker.check(&valid_expr)? {
        Some(data_type) => println!("age > 30 returns: {:?}", data_type),
        None => println!("age > 30 returns: NULL"),
    }

    // Type mismatch
    let invalid_expr = Expression::add_expr(
        Expression::column(0),
        Expression::column(1), // Can't add Int32 and Varchar
    );
    match type_checker.check(&invalid_expr) {
        Ok(_) => println!("Type check passed (unexpected)"),
        Err(e) => println!("Type check failed: {}", e),
    }

    // Example 5: Using expressions as predicates
    println!("\n5. Expression as Predicate");
    println!("--------------------------");

    let predicate = expression_to_predicate(Expression::and(
        Expression::ge(Expression::column(0), Expression::literal(Value::Int32(21))),
        Expression::is_not_null(Expression::column(1)),
    ));

    let test_tuples = vec![
        vec![
            Value::Int32(25),
            Value::String("Alice".into()),
            Value::Boolean(true),
        ],
        vec![
            Value::Int32(18),
            Value::String("Bob".into()),
            Value::Boolean(false),
        ],
        vec![Value::Int32(30), Value::Null, Value::Boolean(true)],
    ];

    for (i, tuple) in test_tuples.iter().enumerate() {
        println!("Tuple {}: {:?} -> {}", i, tuple, predicate(tuple));
    }

    // Example 6: Expression builder pattern
    println!("\n6. Expression Builder Pattern");
    println!("-----------------------------");

    // Build: (age * 2) > 40 OR name = "Admin"
    let complex_expr = Expression::or(
        Expression::gt(
            Expression::mul_expr(Expression::column(0), Expression::literal(Value::Int32(2))),
            Expression::literal(Value::Int32(40)),
        ),
        Expression::eq(
            Expression::column(1),
            Expression::literal(Value::String("Admin".into())),
        ),
    );

    let evaluator = ExpressionEvaluator::new(&tuple_values);
    let result = evaluator.evaluate(&complex_expr)?;
    println!("(age * 2) > 40 OR name = 'Admin': {:?}", result);

    Ok(())
}
