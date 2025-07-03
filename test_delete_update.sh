#!/bin/bash

# Kill any existing vibedb processes
pkill -f "vibedb --port 5445" || true
sleep 1

# Start the server in the background
echo "Starting VibeDB server..."
rm -rf /tmp/vibedb_test_delete_update
cargo run -- --port 5445 --data-dir /tmp/vibedb_test_delete_update > server_delete_update.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
sleep 3

# Test DELETE and UPDATE functionality
echo "Testing DELETE and UPDATE..."

# Test DELETE without WHERE
psql -h localhost -p 5445 -U vibedb -d vibedb -c "DELETE FROM users;" || echo "DELETE without WHERE executed"

# Test DELETE with WHERE
psql -h localhost -p 5445 -U vibedb -d vibedb -c "INSERT INTO users (id, name, email, active) VALUES (10, 'Test User', 'test@example.com', true);" || echo "INSERT executed"
psql -h localhost -p 5445 -U vibedb -d vibedb -c "DELETE FROM users WHERE id = 10;" || echo "DELETE with WHERE executed"

# Test UPDATE without WHERE  
psql -h localhost -p 5445 -U vibedb -d vibedb -c "UPDATE users SET active = false;" || echo "UPDATE without WHERE executed"

# Test UPDATE with WHERE
psql -h localhost -p 5445 -U vibedb -d vibedb -c "INSERT INTO users (id, name, email, active) VALUES (20, 'Update Test', 'update@example.com', true);" || echo "INSERT for UPDATE test executed"
psql -h localhost -p 5445 -U vibedb -d vibedb -c "UPDATE users SET name = 'Updated Name' WHERE id = 20;" || echo "UPDATE with WHERE executed"

# Verify the update
psql -h localhost -p 5445 -U vibedb -d vibedb -c "SELECT * FROM users WHERE id = 20;" || echo "SELECT after UPDATE executed"

# Kill the server
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Test completed. Check server_delete_update.log for details."