#!/bin/bash
# Script to run all tests and show test coverage report

# Ensure script fails on any error
set -e

# Run Maven tests with coverage reporting
mvn clean test

# Show test results summary
echo "Test Results Summary:"
echo "===================="
find target/surefire-reports -name "TEST-*.xml" -exec grep -h "<testcase" {} \; | wc -l | xargs echo "Total test cases:"
find target/surefire-reports -name "TEST-*.xml" -exec grep -h "<failure" {} \; | wc -l | xargs echo "Failed tests:"
find target/surefire-reports -name "TEST-*.xml" -exec grep -h "<error" {} \; | wc -l | xargs echo "Test errors:"
find target/surefire-reports -name "TEST-*.xml" -exec grep -h "<skipped" {} \; | wc -l | xargs echo "Skipped tests:"