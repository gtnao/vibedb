#!/bin/bash
# Check for mod.rs files in the project
# This should be run before commits to ensure no mod.rs files are created

MOD_RS_FILES=$(find src -name "mod.rs" -type f 2>/dev/null)

if [ -n "$MOD_RS_FILES" ]; then
    echo "❌ ERROR: mod.rs files found! This violates project rules."
    echo "Found mod.rs files:"
    echo "$MOD_RS_FILES"
    echo ""
    echo "Please refactor to use parent file module declarations instead."
    echo "See CLAUDE.md for details."
    exit 1
else
    echo "✓ No mod.rs files found. Good!"
    exit 0
fi