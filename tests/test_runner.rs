//! Test runner for tabdiff comprehensive test suite
//! 
//! This provides utilities for running different categories of tests
//! and generating test reports.

use std::process::{Command, Stdio};
use std::time::Instant;

/// Test category for organizing test runs
#[derive(Debug, Clone)]
pub enum TestCategory {
    Unit,
    Integration,
    EdgeCases,
    Functional,
    All,
}

impl TestCategory {
    pub fn test_filter(&self) -> &'static str {
        match self {
            TestCategory::Unit => "unit",
            TestCategory::Integration => "integration",
            TestCategory::EdgeCases => "edge_cases",
            TestCategory::Functional => "functional",
            TestCategory::All => "",
        }
    }
}

/// Test runner configuration
pub struct TestRunner {
    pub verbose: bool,
    pub parallel: bool,
    pub capture_output: bool,
}

impl Default for TestRunner {
    fn default() -> Self {
        Self {
            verbose: false,
            parallel: true,
            capture_output: true,
        }
    }
}

impl TestRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub fn parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    pub fn capture_output(mut self, capture: bool) -> Self {
        self.capture_output = capture;
        self
    }

    /// Run tests for a specific category
    pub fn run_category(&self, category: TestCategory) -> TestResult {
        let start_time = Instant::now();
        
        println!("🧪 Running {:?} tests...", category);
        
        let mut cmd = Command::new("cargo");
        cmd.arg("test");
        
        // Add test filter
        let filter = category.test_filter();
        if !filter.is_empty() {
            cmd.arg(filter);
        }
        
        // Configure output
        if self.verbose {
            cmd.arg("--verbose");
        }
        
        if !self.parallel {
            cmd.arg("--test-threads=1");
        }
        
        if !self.capture_output {
            cmd.arg("--nocapture");
        }
        
        // Add additional flags for better output
        cmd.args(&["--color", "always"]);
        
        // Execute command
        let output = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("Failed to execute cargo test");
        
        let duration = start_time.elapsed();
        let success = output.status.success();
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        // Parse test results
        let (passed, failed, ignored) = self.parse_test_output(&stdout);
        
        if success {
            println!("✅ {:?} tests completed successfully in {:?}", category, duration);
            println!("   Passed: {}, Failed: {}, Ignored: {}", passed, failed, ignored);
        } else {
            println!("❌ {:?} tests failed in {:?}", category, duration);
            println!("   Passed: {}, Failed: {}, Ignored: {}", passed, failed, ignored);
            
            if self.verbose {
                println!("\nSTDOUT:\n{}", stdout);
                println!("\nSTDERR:\n{}", stderr);
            }
        }
        
        TestResult {
            category,
            success,
            duration,
            passed,
            failed,
            ignored,
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
        }
    }
    
    /// Run all test categories
    pub fn run_all(&self) -> Vec<TestResult> {
        let categories = vec![
            TestCategory::Unit,
            TestCategory::Integration,
            TestCategory::EdgeCases,
            TestCategory::Functional,
        ];
        
        let mut results = Vec::new();
        let start_time = Instant::now();
        
        println!("🚀 Running comprehensive tabdiff test suite...\n");
        
        for category in categories {
            let result = self.run_category(category);
            results.push(result);
            println!(); // Add spacing between categories
        }
        
        let total_duration = start_time.elapsed();
        self.print_summary(&results, total_duration);
        
        results
    }
    
    /// Parse test output to extract statistics
    fn parse_test_output(&self, output: &str) -> (usize, usize, usize) {
        let mut total_passed = 0;
        let mut total_failed = 0;
        let mut total_ignored = 0;
        
        for line in output.lines() {
            if line.contains("test result:") {
                // Look for pattern like "test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
                let parts: Vec<&str> = line.split_whitespace().collect();
                for (i, part) in parts.iter().enumerate() {
                    if part == &"passed;" && i > 0 {
                        if let Ok(count) = parts[i - 1].parse::<usize>() {
                            total_passed += count;
                        }
                    } else if part == &"failed;" && i > 0 {
                        if let Ok(count) = parts[i - 1].parse::<usize>() {
                            total_failed += count;
                        }
                    } else if part == &"ignored;" && i > 0 {
                        if let Ok(count) = parts[i - 1].parse::<usize>() {
                            total_ignored += count;
                        }
                    }
                }
            }
        }
        
        (total_passed, total_failed, total_ignored)
    }
    
    /// Print summary of all test results
    fn print_summary(&self, results: &[TestResult], total_duration: std::time::Duration) {
        println!("📊 Test Suite Summary");
        println!("═══════════════════════════════════════");
        
        let mut total_passed = 0;
        let mut total_failed = 0;
        let mut total_ignored = 0;
        let mut categories_passed = 0;
        
        for result in results {
            let status = if result.success { "✅ PASS" } else { "❌ FAIL" };
            println!("{} {:?}: {} passed, {} failed, {} ignored ({:?})", 
                status, result.category, result.passed, result.failed, result.ignored, result.duration);
            
            total_passed += result.passed;
            total_failed += result.failed;
            total_ignored += result.ignored;
            
            if result.success {
                categories_passed += 1;
            }
        }
        
        println!("═══════════════════════════════════════");
        println!("Total: {} passed, {} failed, {} ignored", total_passed, total_failed, total_ignored);
        println!("Categories: {}/{} passed", categories_passed, results.len());
        println!("Duration: {:?}", total_duration);
        
        if total_failed == 0 && categories_passed == results.len() {
            println!("\n🎉 All tests passed! The tabdiff implementation is working correctly.");
        } else {
            println!("\n⚠️  Some tests failed. Please review the failures above.");
        }
    }
}

/// Result of running a test category
#[derive(Debug)]
pub struct TestResult {
    pub category: TestCategory,
    pub success: bool,
    pub duration: std::time::Duration,
    pub passed: usize,
    pub failed: usize,
    pub ignored: usize,
    pub stdout: String,
    pub stderr: String,
}

/// CLI for the test runner
pub fn main() {
    let args: Vec<String> = std::env::args().collect();
    
    let mut runner = TestRunner::new();
    let mut category = TestCategory::All;
    
    // Parse command line arguments
    for arg in &args[1..] {
        match arg.as_str() {
            "--verbose" | "-v" => runner = runner.verbose(true),
            "--no-parallel" => runner = runner.parallel(false),
            "--no-capture" => runner = runner.capture_output(false),
            "--unit" => category = TestCategory::Unit,
            "--integration" => category = TestCategory::Integration,
            "--edge-cases" => category = TestCategory::EdgeCases,
            "--functional" => category = TestCategory::Functional,
            "--all" => category = TestCategory::All,
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {
                println!("Unknown argument: {}", arg);
                print_help();
                return;
            }
        }
    }
    
    // Run tests
    match category {
        TestCategory::All => {
            runner.run_all();
        }
        _ => {
            runner.run_category(category);
        }
    }
}

fn print_help() {
    println!("Tabdiff Test Runner");
    println!("==================");
    println!();
    println!("USAGE:");
    println!("    cargo run --bin test_runner [OPTIONS] [CATEGORY]");
    println!();
    println!("OPTIONS:");
    println!("    -v, --verbose       Enable verbose output");
    println!("    --no-parallel       Run tests sequentially");
    println!("    --no-capture        Don't capture test output");
    println!("    -h, --help          Show this help message");
    println!();
    println!("CATEGORIES:");
    println!("    --unit              Run only unit tests");
    println!("    --integration       Run only integration tests");
    println!("    --edge-cases        Run only edge case tests");
    println!("    --functional        Run only functional tests");
    println!("    --all               Run all test categories (default)");
    println!();
    println!("EXAMPLES:");
    println!("    cargo run --bin test_runner");
    println!("    cargo run --bin test_runner --unit --verbose");
    println!("    cargo run --bin test_runner --edge-cases --no-capture");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_category_filters() {
        assert_eq!(TestCategory::Unit.test_filter(), "unit");
        assert_eq!(TestCategory::Integration.test_filter(), "integration");
        assert_eq!(TestCategory::EdgeCases.test_filter(), "edge_cases");
        assert_eq!(TestCategory::Functional.test_filter(), "functional");
        assert_eq!(TestCategory::All.test_filter(), "");
    }

    #[test]
    fn test_runner_configuration() {
        let runner = TestRunner::new()
            .verbose(true)
            .parallel(false)
            .capture_output(false);
        
        assert!(runner.verbose);
        assert!(!runner.parallel);
        assert!(!runner.capture_output);
    }
}
