#!/usr/bin/env python3
"""
Automated Test Refactor for WASM Coverage

This script automatically refactors backend-agnostic tests to use run_with_both_backends
to maximize WASM test coverage.
"""

import os
import re
import glob
import shutil
from pathlib import Path

# Tests that are known to be platform-specific and should NOT be refactored
PLATFORM_SPECIFIC_TESTS = {
    'test_absolute_path_requirement',  # Tests file path validation
    'temp_db_path',  # Helper function for temp files
    'remove_file_if_file_backend',  # File system operations
    'create_test_db',  # Uses tempfile
    'setup_test_table',  # Helper function
    'measure_sql_execution',  # Performance measurement helper
    'measure_transaction_sql_execution',  # Performance measurement helper
    'run_comprehensive_performance_suite',  # Performance test
}

# Files that contain platform-specific tests and should be skipped
PLATFORM_SPECIFIC_FILES = {
    'engine_tests.rs',  # Uses temp files and file system
    'high_level_api_performance_test.rs',  # Performance tests
    'schema_performance_test.rs',  # Performance tests
    'wasm_tests.rs',  # Already WASM-specific
    'test_helpers.rs',  # Helper functions
}

def is_platform_specific_test(test_name):
    """Check if a test is known to be platform-specific."""
    return test_name in PLATFORM_SPECIFIC_TESTS

def is_platform_specific_file(filename):
    """Check if a file contains platform-specific tests."""
    return filename in PLATFORM_SPECIFIC_FILES

def extract_test_function_info(content, test_match):
    """Extract detailed information about a test function."""
    test_name = test_match.group(1)
    return_type = test_match.group(2)
    function_body = test_match.group(3)
    
    # Check if already uses run_with_both_backends
    already_uses_rwb = 'run_with_both_backends(' in function_body
    
    # Check if it's a simple test (no database operations)
    is_simple_test = not any(keyword in function_body.lower() for keyword in [
        'database::open', 'db.execute', 'db.query', 'database::open'
    ])
    
    return {
        'name': test_name,
        'return_type': return_type,
        'body': function_body,
        'already_uses_rwb': already_uses_rwb,
        'is_simple_test': is_simple_test
    }

def refactor_test_function(test_info):
    """Refactor a test function to use run_with_both_backends."""
    test_name = test_info['name']
    return_type = test_info['return_type']
    body = test_info['body']
    
    # Skip if already uses run_with_both_backends
    if test_info['already_uses_rwb']:
        return None
    
    # Skip simple tests (parser tests, etc.) - they don't need database
    if test_info['is_simple_test']:
        return None
    
    # Extract the actual test logic (remove any existing setup)
    # Look for the core test logic
    lines = body.split('\n')
    test_lines = []
    in_test_logic = False
    
    for line in lines:
        stripped = line.strip()
        # Skip empty lines and comments at the start
        if not stripped or stripped.startswith('//'):
            continue
        
        # Start collecting test logic
        test_lines.append(line)
    
    if not test_lines:
        return None
    
    # Create the refactored function
    refactored_body = f'''    run_with_both_backends("{test_name}", |db_path| {{
{chr(10).join(test_lines)}
        Ok(())
    }})'''
    
    return refactored_body

def refactor_test_file(filepath):
    """Refactor all backend-agnostic tests in a file."""
    print(f"Processing {filepath}...")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Check if file already imports test_helpers
    needs_test_helpers_import = 'use test_helpers::run_with_both_backends;' not in content
    
    # Find all test functions
    # Pattern to match test functions with different return types
    test_pattern = r'#\[test\]\s*\nfn\s+(\w+)\s*\([^)]*\)\s*->\s*(Result<\(\)(?:,\s*tegdb::Error)?>)\s*\{([^}]+)\}'
    test_matches = list(re.finditer(test_pattern, content, re.DOTALL))
    
    if not test_matches:
        print(f"  No test functions found in {filepath}")
        return False
    
    # Process each test function
    modified = False
    new_content = content
    
    for test_match in reversed(test_matches):  # Process in reverse to maintain line numbers
        test_info = extract_test_function_info(content, test_match)
        
        # Skip platform-specific tests
        if is_platform_specific_test(test_info['name']):
            print(f"  Skipping platform-specific test: {test_info['name']}")
            continue
        
        # Skip if already uses run_with_both_backends
        if test_info['already_uses_rwb']:
            print(f"  Already uses run_with_both_backends: {test_info['name']}")
            continue
        
        # Skip simple tests
        if test_info['is_simple_test']:
            print(f"  Skipping simple test (no database): {test_info['name']}")
            continue
        
        # Refactor the test
        refactored_body = refactor_test_function(test_info)
        if refactored_body:
            # Replace the function body
            start_pos = test_match.start()
            end_pos = test_match.end()
            
            # Find the opening brace of the function
            brace_start = content.find('{', start_pos)
            if brace_start != -1:
                # Find the matching closing brace
                brace_count = 0
                brace_end = brace_start
                for i, char in enumerate(content[brace_start:], brace_start):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            brace_end = i + 1
                            break
                
                # Replace the function body
                new_content = (
                    new_content[:brace_start + 1] + 
                    '\n' + refactored_body + '\n' +
                    new_content[brace_end:]
                )
                
                print(f"  Refactored: {test_info['name']}")
                modified = True
    
    # Add test_helpers import if needed
    if modified and needs_test_helpers_import:
        # Find the right place to add the import (after other use statements)
        use_pattern = r'(use\s+.*?;)\s*\n'
        use_matches = list(re.finditer(use_pattern, new_content))
        
        if use_matches:
            # Add after the last use statement
            last_use = use_matches[-1]
            insert_pos = last_use.end()
            new_content = (
                new_content[:insert_pos] + 
                '\nuse test_helpers::run_with_both_backends;\n' +
                new_content[insert_pos:]
            )
        else:
            # Add at the beginning of the file
            new_content = 'use test_helpers::run_with_both_backends;\n\n' + new_content
    
    # Write the modified content back
    if modified:
        # Create backup
        backup_path = filepath + '.backup'
        shutil.copy2(filepath, backup_path)
        print(f"  Created backup: {backup_path}")
        
        with open(filepath, 'w') as f:
            f.write(new_content)
        
        print(f"  Successfully refactored {filepath}")
        return True
    
    return False

def main():
    """Main refactoring function."""
    print("=== Automated Test Refactor for WASM Coverage ===\n")
    
    # Find all test files
    test_files = glob.glob('tests/*.rs')
    test_files = [f for f in test_files if not is_platform_specific_file(os.path.basename(f))]
    
    print(f"Found {len(test_files)} test files to process:")
    for f in test_files:
        print(f"  - {f}")
    print()
    
    # Process each file
    total_refactored = 0
    for test_file in test_files:
        if refactor_test_file(test_file):
            total_refactored += 1
    
    print(f"\n=== Refactoring Complete ===")
    print(f"Refactored {total_refactored} files")
    print(f"Backup files created with .backup extension")
    print(f"\nNext steps:")
    print(f"1. Review the changes")
    print(f"2. Run tests to ensure everything works")
    print(f"3. Regenerate WASM tests: python3 generate_wasm_tests.py")
    print(f"4. Run full test suite: ./run_all_tests.sh")

if __name__ == '__main__':
    main() 