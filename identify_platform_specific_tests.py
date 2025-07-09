#!/usr/bin/env python3
"""
Identify Platform-Specific Tests

This script scans the test files to identify which ones are platform-specific
and need to be refactored to use the high-level Database API.
"""

import os
import re
import glob

def scan_test_files():
    """Scan all test files and categorize them."""
    test_files = glob.glob('tests/*.rs')
    
    platform_specific = []
    backend_agnostic = []
    mixed = []
    
    for test_file in test_files:
        with open(test_file, 'r') as f:
            content = f.read()
        
        # Check for platform-specific indicators
        has_storage_engine = 'StorageEngine::new' in content
        has_file_ops = any(op in content for op in ['fs::remove_file', 'tempfile::', 'PathBuf::from'])
        has_run_with_both_backends = 'run_with_both_backends' in content
        has_database_api = 'Database::open' in content
        
        # Categorize the file
        if has_storage_engine or has_file_ops:
            if has_run_with_both_backends or has_database_api:
                mixed.append(test_file)
            else:
                platform_specific.append(test_file)
        elif has_run_with_both_backends or has_database_api:
            backend_agnostic.append(test_file)
        else:
            # Parser tests or other non-database tests
            backend_agnostic.append(test_file)
    
    return platform_specific, backend_agnostic, mixed

def analyze_platform_specific_files(files):
    """Analyze platform-specific files in detail."""
    results = []
    
    for file_path in files:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Count different types of operations
        storage_engine_ops = len(re.findall(r'StorageEngine::new', content))
        file_ops = len(re.findall(r'fs::remove_file|tempfile::|PathBuf::from', content))
        test_functions = len(re.findall(r'#\[test\]\s*\nfn\s+\w+', content))
        
        # Estimate complexity
        complexity = "Low"
        if storage_engine_ops > 10 or file_ops > 10:
            complexity = "High"
        elif storage_engine_ops > 5 or file_ops > 5:
            complexity = "Medium"
        
        results.append({
            'file': file_path,
            'storage_engine_ops': storage_engine_ops,
            'file_ops': file_ops,
            'test_functions': test_functions,
            'complexity': complexity
        })
    
    return results

def main():
    """Main analysis function."""
    print("=== Platform-Specific Test Analysis ===\n")
    
    platform_specific, backend_agnostic, mixed = scan_test_files()
    
    print(f"📊 Test File Analysis:")
    print(f"  Platform-specific tests: {len(platform_specific)}")
    print(f"  Backend-agnostic tests: {len(backend_agnostic)}")
    print(f"  Mixed tests: {len(mixed)}")
    print(f"  Total test files: {len(platform_specific) + len(backend_agnostic) + len(mixed)}")
    
    print(f"\n🔧 Platform-Specific Files (Need Refactoring):")
    if platform_specific:
        detailed = analyze_platform_specific_files(platform_specific)
        for item in sorted(detailed, key=lambda x: x['complexity']):
            print(f"  {item['file']}")
            print(f"    - Complexity: {item['complexity']}")
            print(f"    - StorageEngine ops: {item['storage_engine_ops']}")
            print(f"    - File ops: {item['file_ops']}")
            print(f"    - Test functions: {item['test_functions']}")
            print()
    else:
        print("  None found!")
    
    print(f"\n✅ Backend-Agnostic Files (Already WASM-ready):")
    for file_path in sorted(backend_agnostic):
        print(f"  {file_path}")
    
    print(f"\n🔄 Mixed Files (Partially Refactored):")
    for file_path in sorted(mixed):
        print(f"  {file_path}")
    
    print(f"\n📋 Refactoring Priority:")
    if platform_specific:
        detailed = analyze_platform_specific_files(platform_specific)
        
        # Sort by complexity and number of operations
        sorted_files = sorted(detailed, key=lambda x: (
            {'Low': 1, 'Medium': 2, 'High': 3}[x['complexity']],
            -(x['storage_engine_ops'] + x['file_ops'])
        ))
        
        print(f"  High Priority (Easy):")
        for item in sorted_files:
            if item['complexity'] == 'Low':
                print(f"    - {item['file']} ({item['storage_engine_ops']} ops)")
        
        print(f"  Medium Priority:")
        for item in sorted_files:
            if item['complexity'] == 'Medium':
                print(f"    - {item['file']} ({item['storage_engine_ops']} ops)")
        
        print(f"  Low Priority (Complex):")
        for item in sorted_files:
            if item['complexity'] == 'High':
                print(f"    - {item['file']} ({item['storage_engine_ops']} ops)")
    
    print(f"\n🎯 Next Steps:")
    print(f"  1. Start with high-priority files (low complexity)")
    print(f"  2. Follow the refactoring guide in PLATFORM_SPECIFIC_REFACTOR_GUIDE.md")
    print(f"  3. Test each refactored file individually")
    print(f"  4. Run python3 generate_wasm_tests.py to regenerate WASM tests")
    print(f"  5. Use ./run_all_tests.sh to verify everything works")

if __name__ == '__main__':
    main() 