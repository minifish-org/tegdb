//! Extension system for TegDB
//!
//! This module provides a PostgreSQL-inspired extension system that allows users
//! to extend TegDB with custom functions, aggregate functions, and types.
//!
//! # Example
//!
//! ```rust,ignore
//! use tegdb::{Database, Extension, ScalarFunction, SqlValue, FunctionSignature, ArgType, DataType};
//!
//! // Define a custom function
//! struct ReverseFunction;
//!
//! impl ScalarFunction for ReverseFunction {
//!     fn name(&self) -> &'static str { "REVERSE" }
//!     
//!     fn signature(&self) -> FunctionSignature {
//!         FunctionSignature::new(vec![ArgType::Exact(DataType::Text(None))], DataType::Text(None))
//!     }
//!     
//!     fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
//!         match &args[0] {
//!             SqlValue::Text(s) => Ok(SqlValue::Text(s.chars().rev().collect())),
//!             SqlValue::Null => Ok(SqlValue::Null),
//!             _ => Err("REVERSE requires text argument".to_string()),
//!         }
//!     }
//! }
//!
//! // Create and register extension
//! struct MyExtension;
//!
//! impl Extension for MyExtension {
//!     fn name(&self) -> &'static str { "my_extension" }
//!     fn version(&self) -> &'static str { "0.1.0" }
//!     
//!     fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
//!         vec![Box::new(ReverseFunction)]
//!     }
//! }
//!
//! // Register with database
//! let mut db = Database::open("file:///path/to/db.teg")?;
//! db.register_extension(Box::new(MyExtension))?;
//!
//! // Now use in SQL
//! db.query("SELECT REVERSE('hello')")?; // Returns "olleh"
//! ```

use crate::parser::{DataType, SqlValue};
use crate::Error;
use libloading::{Library, Symbol};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// Error type for extension operations
#[derive(Debug, Clone)]
pub enum ExtensionError {
    /// Function not found
    FunctionNotFound(String),
    /// Type mismatch in function arguments
    TypeMismatch {
        function: String,
        expected: String,
        got: String,
    },
    /// Wrong number of arguments
    ArgumentCountMismatch {
        function: String,
        expected: usize,
        got: usize,
    },
    /// Extension already registered
    AlreadyRegistered(String),
    /// Execution error
    ExecutionError(String),
    /// Other error
    Other(String),
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtensionError::FunctionNotFound(name) => {
                write!(f, "Function '{}' not found", name)
            }
            ExtensionError::TypeMismatch {
                function,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Type mismatch in function '{}': expected {}, got {}",
                    function, expected, got
                )
            }
            ExtensionError::ArgumentCountMismatch {
                function,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Function '{}' expects {} arguments, got {}",
                    function, expected, got
                )
            }
            ExtensionError::AlreadyRegistered(name) => {
                write!(f, "Extension '{}' is already registered", name)
            }
            ExtensionError::ExecutionError(msg) => {
                write!(f, "Execution error: {}", msg)
            }
            ExtensionError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ExtensionError {}

/// Result type for extension operations
pub type ExtensionResult<T> = std::result::Result<T, ExtensionError>;

// ============================================================================
// Function Signatures
// ============================================================================

/// Argument type specification for function signatures
#[derive(Debug, Clone)]
pub enum ArgType {
    /// Exact type match required
    Exact(DataType),
    /// Any type accepted
    Any,
    /// One of the listed types
    OneOf(Vec<DataType>),
    /// Numeric type (Integer or Real)
    Numeric,
    /// Text-like type
    TextLike,
}

impl ArgType {
    /// Check if a SqlValue matches this argument type
    pub fn matches(&self, value: &SqlValue) -> bool {
        match self {
            ArgType::Exact(dt) => Self::value_matches_datatype(value, dt),
            ArgType::Any => true,
            ArgType::OneOf(types) => types
                .iter()
                .any(|dt| Self::value_matches_datatype(value, dt)),
            ArgType::Numeric => matches!(value, SqlValue::Integer(_) | SqlValue::Real(_)),
            ArgType::TextLike => matches!(value, SqlValue::Text(_)),
        }
    }

    fn value_matches_datatype(value: &SqlValue, dt: &DataType) -> bool {
        match (value, dt) {
            (SqlValue::Integer(_), DataType::Integer) => true,
            (SqlValue::Real(_), DataType::Real) => true,
            (SqlValue::Text(_), DataType::Text(_)) => true,
            (SqlValue::Vector(_), DataType::Vector(_)) => true,
            (SqlValue::Null, _) => true, // NULL matches any type
            _ => false,
        }
    }

    /// Get a human-readable description of this type
    pub fn description(&self) -> String {
        match self {
            ArgType::Exact(dt) => format!("{:?}", dt),
            ArgType::Any => "any".to_string(),
            ArgType::OneOf(types) => {
                let type_strs: Vec<String> = types.iter().map(|t| format!("{:?}", t)).collect();
                type_strs.join(" | ")
            }
            ArgType::Numeric => "numeric (INTEGER or REAL)".to_string(),
            ArgType::TextLike => "TEXT".to_string(),
        }
    }
}

/// Function signature defining argument and return types
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// Expected argument types
    pub arg_types: Vec<ArgType>,
    /// Return type
    pub return_type: DataType,
    /// Whether the function accepts variable arguments
    pub variadic: bool,
    /// Minimum number of arguments (for variadic functions)
    pub min_args: Option<usize>,
}

impl FunctionSignature {
    /// Create a new function signature
    pub fn new(arg_types: Vec<ArgType>, return_type: DataType) -> Self {
        Self {
            arg_types,
            return_type,
            variadic: false,
            min_args: None,
        }
    }

    /// Create a variadic function signature
    pub fn variadic(arg_type: ArgType, min_args: usize, return_type: DataType) -> Self {
        Self {
            arg_types: vec![arg_type],
            return_type,
            variadic: true,
            min_args: Some(min_args),
        }
    }

    /// Validate arguments against this signature
    pub fn validate(&self, args: &[SqlValue]) -> ExtensionResult<()> {
        if self.variadic {
            let min = self.min_args.unwrap_or(0);
            if args.len() < min {
                return Err(ExtensionError::ArgumentCountMismatch {
                    function: "".to_string(),
                    expected: min,
                    got: args.len(),
                });
            }
            // Check all args against the single type pattern
            let arg_type = &self.arg_types[0];
            for (i, arg) in args.iter().enumerate() {
                if !arg_type.matches(arg) {
                    return Err(ExtensionError::TypeMismatch {
                        function: "".to_string(),
                        expected: arg_type.description(),
                        got: format!("{:?} (argument {})", arg, i + 1),
                    });
                }
            }
        } else {
            if args.len() != self.arg_types.len() {
                return Err(ExtensionError::ArgumentCountMismatch {
                    function: "".to_string(),
                    expected: self.arg_types.len(),
                    got: args.len(),
                });
            }
            for (i, (arg, expected)) in args.iter().zip(self.arg_types.iter()).enumerate() {
                if !expected.matches(arg) {
                    return Err(ExtensionError::TypeMismatch {
                        function: "".to_string(),
                        expected: expected.description(),
                        got: format!("{:?} (argument {})", arg, i + 1),
                    });
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// Scalar Functions
// ============================================================================

/// Trait for scalar functions (value -> value)
///
/// Scalar functions take zero or more input values and produce a single output value.
/// They are called once per row in query execution.
pub trait ScalarFunction: Send + Sync {
    /// The function name (case-insensitive in SQL)
    fn name(&self) -> &'static str;

    /// Function signature for type checking
    fn signature(&self) -> FunctionSignature;

    /// Execute the function with given arguments
    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String>;

    /// Whether the function is deterministic (same inputs always produce same output)
    /// Deterministic functions can be constant-folded during query planning.
    fn is_deterministic(&self) -> bool {
        true
    }

    /// Optional description for documentation
    fn description(&self) -> Option<&'static str> {
        None
    }
}

// ============================================================================
// Aggregate Functions
// ============================================================================

/// State for aggregate function accumulation
pub trait AggregateState: Send {
    /// Add a value to the accumulator
    fn accumulate(&mut self, value: &SqlValue) -> Result<(), String>;

    /// Merge another state into this one (for parallel execution)
    fn merge(&mut self, other: &dyn AggregateState) -> Result<(), String>;

    /// Produce the final result
    fn finalize(&self) -> Result<SqlValue, String>;

    /// Reset state for reuse
    fn reset(&mut self);

    /// Clone the state (for parallel execution)
    fn clone_state(&self) -> Box<dyn AggregateState>;
}

/// Trait for aggregate functions (values -> single value)
///
/// Aggregate functions accumulate values across multiple rows and produce
/// a single result (e.g., SUM, COUNT, AVG).
pub trait AggregateFunction: Send + Sync {
    /// The function name (case-insensitive in SQL)
    fn name(&self) -> &'static str;

    /// Function signature for type checking
    fn signature(&self) -> FunctionSignature;

    /// Create a new accumulator state
    fn create_state(&self) -> Box<dyn AggregateState>;

    /// Optional description for documentation
    fn description(&self) -> Option<&'static str> {
        None
    }
}

// ============================================================================
// Extension Trait
// ============================================================================

/// Main extension trait
///
/// Implement this trait to create a TegDB extension. Extensions can provide:
/// - Scalar functions
/// - Aggregate functions
/// - Lifecycle hooks
pub trait Extension: Send + Sync {
    /// Unique identifier for the extension
    fn name(&self) -> &'static str;

    /// Semantic version string
    fn version(&self) -> &'static str;

    /// Scalar functions provided by this extension
    fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
        Vec::new()
    }

    /// Aggregate functions provided by this extension
    fn aggregate_functions(&self) -> Vec<Box<dyn AggregateFunction>> {
        Vec::new()
    }

    /// Called when the extension is loaded
    fn on_load(&self) -> Result<(), String> {
        Ok(())
    }

    /// Called when the extension is unloaded
    fn on_unload(&self) -> Result<(), String> {
        Ok(())
    }

    /// Optional description for documentation
    fn description(&self) -> Option<&'static str> {
        None
    }

    /// List of other extensions this extension depends on
    fn dependencies(&self) -> Vec<&'static str> {
        Vec::new()
    }
}

// ============================================================================
// Extension Registry
// ============================================================================

/// Registry for managing loaded extensions and their functions
pub struct ExtensionRegistry {
    /// Registered extensions by name
    extensions: HashMap<String, Box<dyn Extension>>,
    /// Scalar functions indexed by uppercase name
    scalar_functions: HashMap<String, Box<dyn ScalarFunction>>,
    /// Aggregate functions indexed by uppercase name
    aggregate_functions: HashMap<String, Box<dyn AggregateFunction>>,
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtensionRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            extensions: HashMap::new(),
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
        }
    }

    /// Register an extension
    pub fn register(&mut self, extension: Box<dyn Extension>) -> ExtensionResult<()> {
        let name = extension.name().to_string();

        // Check if already registered
        if self.extensions.contains_key(&name) {
            return Err(ExtensionError::AlreadyRegistered(name));
        }

        // Check dependencies
        for dep in extension.dependencies() {
            if !self.extensions.contains_key(dep) {
                return Err(ExtensionError::Other(format!(
                    "Extension '{}' requires '{}' which is not loaded",
                    name, dep
                )));
            }
        }

        // Call on_load hook
        extension.on_load().map_err(|e| {
            ExtensionError::Other(format!("Failed to load extension '{}': {}", name, e))
        })?;

        // Register scalar functions
        for func in extension.scalar_functions() {
            let func_name = func.name().to_uppercase();
            if self.scalar_functions.contains_key(&func_name) {
                return Err(ExtensionError::Other(format!(
                    "Scalar function '{}' is already registered",
                    func_name
                )));
            }
            self.scalar_functions.insert(func_name, func);
        }

        // Register aggregate functions
        for func in extension.aggregate_functions() {
            let func_name = func.name().to_uppercase();
            if self.aggregate_functions.contains_key(&func_name) {
                return Err(ExtensionError::Other(format!(
                    "Aggregate function '{}' is already registered",
                    func_name
                )));
            }
            self.aggregate_functions.insert(func_name, func);
        }

        self.extensions.insert(name, extension);
        Ok(())
    }

    /// Unregister an extension by name
    pub fn unregister(&mut self, name: &str) -> ExtensionResult<()> {
        let extension = self
            .extensions
            .remove(name)
            .ok_or_else(|| ExtensionError::Other(format!("Extension '{}' not found", name)))?;

        // Remove scalar functions
        for func in extension.scalar_functions() {
            self.scalar_functions.remove(&func.name().to_uppercase());
        }

        // Remove aggregate functions
        for func in extension.aggregate_functions() {
            self.aggregate_functions.remove(&func.name().to_uppercase());
        }

        // Call on_unload hook
        extension.on_unload().map_err(|e| {
            ExtensionError::Other(format!("Error unloading extension '{}': {}", name, e))
        })?;

        Ok(())
    }

    /// Get a scalar function by name (case-insensitive)
    pub fn get_scalar_function(&self, name: &str) -> Option<&dyn ScalarFunction> {
        self.scalar_functions
            .get(&name.to_uppercase())
            .map(|f| f.as_ref())
    }

    /// Get an aggregate function by name (case-insensitive)
    pub fn get_aggregate_function(&self, name: &str) -> Option<&dyn AggregateFunction> {
        self.aggregate_functions
            .get(&name.to_uppercase())
            .map(|f| f.as_ref())
    }

    /// Check if a scalar function exists
    pub fn has_scalar_function(&self, name: &str) -> bool {
        self.scalar_functions.contains_key(&name.to_uppercase())
    }

    /// Check if an aggregate function exists
    pub fn has_aggregate_function(&self, name: &str) -> bool {
        self.aggregate_functions.contains_key(&name.to_uppercase())
    }

    /// Execute a scalar function by name
    pub fn execute_scalar(&self, name: &str, args: &[SqlValue]) -> ExtensionResult<SqlValue> {
        let func = self
            .get_scalar_function(name)
            .ok_or_else(|| ExtensionError::FunctionNotFound(name.to_string()))?;

        // Validate arguments
        let mut validation_result = func.signature().validate(args);
        if let Err(ref mut e) = validation_result {
            // Add function name to error
            match e {
                ExtensionError::ArgumentCountMismatch { function, .. } => {
                    *function = name.to_string();
                }
                ExtensionError::TypeMismatch { function, .. } => {
                    *function = name.to_string();
                }
                _ => {}
            }
        }
        validation_result?;

        // Execute
        func.execute(args)
            .map_err(|e| ExtensionError::ExecutionError(format!("{}: {}", name, e)))
    }

    /// List all registered extensions
    pub fn list_extensions(&self) -> Vec<(&str, &str)> {
        self.extensions
            .values()
            .map(|ext| (ext.name(), ext.version()))
            .collect()
    }

    /// List all registered scalar functions
    pub fn list_scalar_functions(&self) -> Vec<&str> {
        self.scalar_functions.keys().map(|s| s.as_str()).collect()
    }

    /// List all registered aggregate functions
    pub fn list_aggregate_functions(&self) -> Vec<&str> {
        self.aggregate_functions
            .keys()
            .map(|s| s.as_str())
            .collect()
    }

    /// Check if a function name (scalar or aggregate) is registered
    pub fn has_function(&self, name: &str) -> bool {
        let upper = name.to_uppercase();
        self.scalar_functions.contains_key(&upper) || self.aggregate_functions.contains_key(&upper)
    }

    /// Check if an extension is registered by name
    pub fn has_extension(&self, name: &str) -> bool {
        self.extensions.contains_key(name)
    }
}

// ============================================================================
// Extension Factory for Dynamic Library Loading
// ============================================================================

/// Factory for creating extensions from dynamic libraries or built-in extensions
pub struct ExtensionFactory {
    search_paths: Vec<PathBuf>,
}

/// Wrapper to hold extension in a C-compatible way
/// This is needed because trait objects cannot be passed across FFI boundaries
/// Extension developers should use this when implementing the create_extension entry point
pub struct ExtensionWrapper {
    pub extension: Box<dyn Extension>,
}

/// Type alias for the extension creation function exported from dynamic libraries
/// Returns a pointer to an ExtensionWrapper
type CreateExtensionFn = unsafe extern "C" fn() -> *mut ExtensionWrapper;

impl ExtensionFactory {
    /// Create a new extension factory with search paths
    pub fn new(search_paths: Vec<PathBuf>) -> Self {
        Self { search_paths }
    }

    /// Get default search paths
    pub fn default_search_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        // Current directory extensions
        paths.push(PathBuf::from("./extensions"));

        // User home directory
        if let Some(home) = std::env::var_os("HOME") {
            let mut home_path = PathBuf::from(home);
            home_path.push(".tegdb");
            home_path.push("extensions");
            paths.push(home_path);
        }

        // Platform-specific system paths
        #[cfg(target_os = "linux")]
        {
            paths.push(PathBuf::from("/usr/local/lib/tegdb/extensions"));
            paths.push(PathBuf::from("/usr/lib/tegdb/extensions"));
        }

        #[cfg(target_os = "macos")]
        {
            paths.push(PathBuf::from("/usr/local/lib/tegdb/extensions"));
            paths.push(PathBuf::from("/opt/homebrew/lib/tegdb/extensions"));
        }

        #[cfg(target_os = "windows")]
        {
            if let Some(program_files) = std::env::var_os("ProgramFiles") {
                let mut pf_path = PathBuf::from(program_files);
                pf_path.push("TegDB");
                pf_path.push("extensions");
                paths.push(pf_path);
            }
        }

        paths
    }

    /// Load extension from a dynamic library file
    pub fn load_from_path(&self, path: &Path) -> std::result::Result<Box<dyn Extension>, Error> {
        unsafe {
            let library = Library::new(path).map_err(|e| {
                Error::Other(format!(
                    "Failed to load extension library '{}': {}",
                    path.display(),
                    e
                ))
            })?;

            // Get the create_extension symbol
            let create_fn: Symbol<CreateExtensionFn> =
                library.get(b"create_extension").map_err(|e| {
                    Error::Other(format!(
                        "Failed to resolve extension entry point 'create_extension' in '{}': {}",
                        path.display(),
                        e
                    ))
                })?;

            // Call the function to create the extension
            let wrapper_ptr = create_fn();
            if wrapper_ptr.is_null() {
                return Err(Error::Other(format!(
                    "Extension creation function returned null pointer in '{}'",
                    path.display()
                )));
            }

            // Convert raw pointer to Box<ExtensionWrapper> and extract the extension
            // SAFETY: The extension library must return a valid pointer to an ExtensionWrapper
            // that was created with Box::into_raw. This is part of the extension API contract.
            let wrapper = Box::from_raw(wrapper_ptr);
            let extension = wrapper.extension;

            // Note: We intentionally leak the library here because:
            // 1. The extension may hold references to code in the library
            // 2. We can't unload the library while the extension is in use
            // 3. The library will be unloaded when the process exits
            std::mem::forget(library);

            Ok(extension)
        }
    }

    /// Load extension by name, searching in configured paths
    pub fn load_from_name(&self, name: &str) -> std::result::Result<Box<dyn Extension>, Error> {
        // Try built-in extensions first
        if let Some(ext) = self.create_builtin_extension(name) {
            return Ok(ext);
        }

        // Search for dynamic library
        let library_names = Self::get_library_names(name);

        for search_path in &self.search_paths {
            for lib_name in &library_names {
                let full_path = search_path.join(lib_name);
                if full_path.exists() {
                    return self.load_from_path(&full_path);
                }
            }
        }

        Err(Error::Other(format!(
            "Extension '{}' not found in search paths: {:?}",
            name, self.search_paths
        )))
    }

    /// Create a built-in extension by name
    pub fn create_builtin_extension(&self, name: &str) -> Option<Box<dyn Extension>> {
        match name {
            "tegdb_string" => Some(Box::new(StringFunctionsExtension)),
            "tegdb_math" => Some(Box::new(MathFunctionsExtension)),
            _ => None,
        }
    }

    /// List all available extensions (built-in + found in search paths)
    pub fn list_available(&self) -> Vec<String> {
        let mut extensions = Vec::new();

        // Add built-in extensions
        extensions.push("tegdb_string".to_string());
        extensions.push("tegdb_math".to_string());

        // Scan search paths for dynamic libraries
        for search_path in &self.search_paths {
            if let Ok(entries) = std::fs::read_dir(search_path) {
                for entry in entries.flatten() {
                    if let Some(file_name) = entry.file_name().to_str() {
                        // Check if it's a library file
                        if Self::is_library_file(file_name) {
                            // Extract extension name from library filename
                            if let Some(ext_name) = Self::extract_extension_name(file_name) {
                                if !extensions.contains(&ext_name) {
                                    extensions.push(ext_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        extensions
    }

    /// Get platform-specific library names for an extension
    fn get_library_names(name: &str) -> Vec<String> {
        #[cfg(target_os = "linux")]
        {
            vec![format!("lib{}.so", name)]
        }

        #[cfg(target_os = "macos")]
        {
            vec![format!("lib{}.dylib", name)]
        }

        #[cfg(target_os = "windows")]
        {
            vec![format!("{}.dll", name)]
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            vec![
                format!("lib{}.so", name),
                format!("lib{}.dylib", name),
                format!("{}.dll", name),
            ]
        }
    }

    /// Check if a filename is a library file
    fn is_library_file(filename: &str) -> bool {
        filename.ends_with(".so") || filename.ends_with(".dylib") || filename.ends_with(".dll")
    }

    /// Extract extension name from library filename
    fn extract_extension_name(filename: &str) -> Option<String> {
        if let Some(stripped) = filename.strip_prefix("lib") {
            stripped
                .strip_suffix(".so")
                .or_else(|| stripped.strip_suffix(".dylib"))
                .map(|name| name.to_string())
        } else {
            filename.strip_suffix(".dll").map(|name| name.to_string())
        }
    }
}

// ============================================================================
// Built-in Functions (Optional - can be moved to separate module)
// ============================================================================

/// Built-in string functions extension
pub struct StringFunctionsExtension;

impl Extension for StringFunctionsExtension {
    fn name(&self) -> &'static str {
        "tegdb_string"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Built-in string manipulation functions")
    }

    fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
        vec![
            Box::new(UpperFunction),
            Box::new(LowerFunction),
            Box::new(LengthFunction),
            Box::new(TrimFunction),
            Box::new(LTrimFunction),
            Box::new(RTrimFunction),
            Box::new(SubstrFunction),
            Box::new(ReplaceFunction),
            Box::new(ConcatFunction),
            Box::new(ReverseFunction),
        ]
    }
}

// UPPER(text) -> TEXT
struct UpperFunction;

impl ScalarFunction for UpperFunction {
    fn name(&self) -> &'static str {
        "UPPER"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Text(s.to_uppercase())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("UPPER requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Convert text to uppercase")
    }
}

// LOWER(text) -> TEXT
struct LowerFunction;

impl ScalarFunction for LowerFunction {
    fn name(&self) -> &'static str {
        "LOWER"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Text(s.to_lowercase())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("LOWER requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Convert text to lowercase")
    }
}

// LENGTH(text) -> INTEGER
struct LengthFunction;

impl ScalarFunction for LengthFunction {
    fn name(&self) -> &'static str {
        "LENGTH"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Integer)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Integer(s.chars().count() as i64)),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("LENGTH requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Return the length of text in characters")
    }
}

// TRIM(text) -> TEXT
struct TrimFunction;

impl ScalarFunction for TrimFunction {
    fn name(&self) -> &'static str {
        "TRIM"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Text(s.trim().to_string())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("TRIM requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Remove leading and trailing whitespace")
    }
}

// LTRIM(text) -> TEXT
struct LTrimFunction;

impl ScalarFunction for LTrimFunction {
    fn name(&self) -> &'static str {
        "LTRIM"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Text(s.trim_start().to_string())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("LTRIM requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Remove leading whitespace")
    }
}

// RTRIM(text) -> TEXT
struct RTrimFunction;

impl ScalarFunction for RTrimFunction {
    fn name(&self) -> &'static str {
        "RTRIM"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Text(s.trim_end().to_string())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("RTRIM requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Remove trailing whitespace")
    }
}

// SUBSTR(text, start, [length]) -> TEXT
struct SubstrFunction;

impl ScalarFunction for SubstrFunction {
    fn name(&self) -> &'static str {
        "SUBSTR"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            arg_types: vec![ArgType::TextLike, ArgType::Numeric, ArgType::Numeric],
            return_type: DataType::Text(None),
            variadic: false,
            min_args: Some(2), // length is optional
        }
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        let text = match &args[0] {
            SqlValue::Text(s) => s,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("SUBSTR first argument must be text".to_string()),
        };

        let start = match &args[1] {
            SqlValue::Integer(i) => (*i as usize).saturating_sub(1), // SQL is 1-indexed
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("SUBSTR second argument must be integer".to_string()),
        };

        let chars: Vec<char> = text.chars().collect();

        if start >= chars.len() {
            return Ok(SqlValue::Text(String::new()));
        }

        let result = if args.len() > 2 {
            let length = match &args[2] {
                SqlValue::Integer(i) => *i as usize,
                SqlValue::Null => return Ok(SqlValue::Null),
                _ => return Err("SUBSTR third argument must be integer".to_string()),
            };
            chars[start..].iter().take(length).collect()
        } else {
            chars[start..].iter().collect()
        };

        Ok(SqlValue::Text(result))
    }

    fn description(&self) -> Option<&'static str> {
        Some("Extract substring: SUBSTR(text, start) or SUBSTR(text, start, length)")
    }
}

// REPLACE(text, from, to) -> TEXT
struct ReplaceFunction;

impl ScalarFunction for ReplaceFunction {
    fn name(&self) -> &'static str {
        "REPLACE"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(
            vec![ArgType::TextLike, ArgType::TextLike, ArgType::TextLike],
            DataType::Text(None),
        )
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        let text = match &args[0] {
            SqlValue::Text(s) => s,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("REPLACE first argument must be text".to_string()),
        };

        let from = match &args[1] {
            SqlValue::Text(s) => s,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("REPLACE second argument must be text".to_string()),
        };

        let to = match &args[2] {
            SqlValue::Text(s) => s,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("REPLACE third argument must be text".to_string()),
        };

        Ok(SqlValue::Text(text.replace(from, to)))
    }

    fn description(&self) -> Option<&'static str> {
        Some("Replace all occurrences of 'from' with 'to' in text")
    }
}

// CONCAT(text, text, ...) -> TEXT
struct ConcatFunction;

impl ScalarFunction for ConcatFunction {
    fn name(&self) -> &'static str {
        "CONCAT"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::variadic(ArgType::Any, 1, DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        let mut result = String::new();
        for arg in args {
            match arg {
                SqlValue::Text(s) => result.push_str(s),
                SqlValue::Integer(i) => result.push_str(&i.to_string()),
                SqlValue::Real(r) => result.push_str(&r.to_string()),
                SqlValue::Null => {} // NULL is ignored in CONCAT
                SqlValue::Vector(v) => result.push_str(&format!("{:?}", v)),
                SqlValue::Parameter(_) => {
                    return Err("Unbound parameter in CONCAT".to_string());
                }
            }
        }
        Ok(SqlValue::Text(result))
    }

    fn description(&self) -> Option<&'static str> {
        Some("Concatenate multiple values into a single text string")
    }
}

// REVERSE(text) -> TEXT
struct ReverseFunction;

impl ScalarFunction for ReverseFunction {
    fn name(&self) -> &'static str {
        "REVERSE"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None))
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Text(s) => Ok(SqlValue::Text(s.chars().rev().collect())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("REVERSE requires text argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Reverse the characters in a text string")
    }
}

/// Built-in math functions extension
pub struct MathFunctionsExtension;

impl Extension for MathFunctionsExtension {
    fn name(&self) -> &'static str {
        "tegdb_math"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Built-in mathematical functions")
    }

    fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
        vec![
            Box::new(AbsFunction),
            Box::new(CeilFunction),
            Box::new(FloorFunction),
            Box::new(RoundFunction),
            Box::new(SqrtFunction),
            Box::new(PowFunction),
            Box::new(ModFunction),
            Box::new(SignFunction),
        ]
    }
}

// ABS(numeric) -> numeric
struct AbsFunction;

impl ScalarFunction for AbsFunction {
    fn name(&self) -> &'static str {
        "ABS"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric], DataType::Real)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(i.abs())),
            SqlValue::Real(r) => Ok(SqlValue::Real(r.abs())),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("ABS requires numeric argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Return the absolute value")
    }
}

// CEIL(numeric) -> INTEGER
struct CeilFunction;

impl ScalarFunction for CeilFunction {
    fn name(&self) -> &'static str {
        "CEIL"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric], DataType::Integer)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(*i)),
            SqlValue::Real(r) => Ok(SqlValue::Integer(r.ceil() as i64)),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("CEIL requires numeric argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Round up to nearest integer")
    }
}

// FLOOR(numeric) -> INTEGER
struct FloorFunction;

impl ScalarFunction for FloorFunction {
    fn name(&self) -> &'static str {
        "FLOOR"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric], DataType::Integer)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(*i)),
            SqlValue::Real(r) => Ok(SqlValue::Integer(r.floor() as i64)),
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("FLOOR requires numeric argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Round down to nearest integer")
    }
}

// ROUND(numeric, [decimals]) -> REAL
struct RoundFunction;

impl ScalarFunction for RoundFunction {
    fn name(&self) -> &'static str {
        "ROUND"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            arg_types: vec![ArgType::Numeric, ArgType::Numeric],
            return_type: DataType::Real,
            variadic: false,
            min_args: Some(1),
        }
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        let value = match &args[0] {
            SqlValue::Integer(i) => *i as f64,
            SqlValue::Real(r) => *r,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("ROUND requires numeric argument".to_string()),
        };

        let decimals = if args.len() > 1 {
            match &args[1] {
                SqlValue::Integer(i) => *i as i32,
                SqlValue::Null => return Ok(SqlValue::Null),
                _ => return Err("ROUND second argument must be integer".to_string()),
            }
        } else {
            0
        };

        let multiplier = 10_f64.powi(decimals);
        let rounded = (value * multiplier).round() / multiplier;

        Ok(SqlValue::Real(rounded))
    }

    fn description(&self) -> Option<&'static str> {
        Some("Round to specified decimal places (default 0)")
    }
}

// SQRT(numeric) -> REAL
struct SqrtFunction;

impl ScalarFunction for SqrtFunction {
    fn name(&self) -> &'static str {
        "SQRT"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric], DataType::Real)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        let value = match &args[0] {
            SqlValue::Integer(i) => *i as f64,
            SqlValue::Real(r) => *r,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("SQRT requires numeric argument".to_string()),
        };

        if value < 0.0 {
            return Err("SQRT of negative number is undefined".to_string());
        }

        Ok(SqlValue::Real(value.sqrt()))
    }

    fn description(&self) -> Option<&'static str> {
        Some("Return the square root")
    }
}

// POW(base, exponent) -> REAL
struct PowFunction;

impl ScalarFunction for PowFunction {
    fn name(&self) -> &'static str {
        "POW"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric, ArgType::Numeric], DataType::Real)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        let base = match &args[0] {
            SqlValue::Integer(i) => *i as f64,
            SqlValue::Real(r) => *r,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("POW base must be numeric".to_string()),
        };

        let exponent = match &args[1] {
            SqlValue::Integer(i) => *i as f64,
            SqlValue::Real(r) => *r,
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => return Err("POW exponent must be numeric".to_string()),
        };

        Ok(SqlValue::Real(base.powf(exponent)))
    }

    fn description(&self) -> Option<&'static str> {
        Some("Return base raised to the power of exponent")
    }
}

// MOD(a, b) -> numeric
struct ModFunction;

impl ScalarFunction for ModFunction {
    fn name(&self) -> &'static str {
        "MOD"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric, ArgType::Numeric], DataType::Real)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match (&args[0], &args[1]) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if *b == 0 {
                    return Err("MOD by zero".to_string());
                }
                Ok(SqlValue::Integer(a % b))
            }
            (SqlValue::Real(a), SqlValue::Real(b)) => {
                if *b == 0.0 {
                    return Err("MOD by zero".to_string());
                }
                Ok(SqlValue::Real(a % b))
            }
            (SqlValue::Integer(a), SqlValue::Real(b)) => {
                if *b == 0.0 {
                    return Err("MOD by zero".to_string());
                }
                Ok(SqlValue::Real((*a as f64) % b))
            }
            (SqlValue::Real(a), SqlValue::Integer(b)) => {
                if *b == 0 {
                    return Err("MOD by zero".to_string());
                }
                Ok(SqlValue::Real(a % (*b as f64)))
            }
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            _ => Err("MOD requires numeric arguments".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Return the remainder of division")
    }
}

// SIGN(numeric) -> INTEGER
struct SignFunction;

impl ScalarFunction for SignFunction {
    fn name(&self) -> &'static str {
        "SIGN"
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature::new(vec![ArgType::Numeric], DataType::Integer)
    }

    fn execute(&self, args: &[SqlValue]) -> Result<SqlValue, String> {
        match &args[0] {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(i.signum())),
            SqlValue::Real(r) => {
                if *r > 0.0 {
                    Ok(SqlValue::Integer(1))
                } else if *r < 0.0 {
                    Ok(SqlValue::Integer(-1))
                } else {
                    Ok(SqlValue::Integer(0))
                }
            }
            SqlValue::Null => Ok(SqlValue::Null),
            _ => Err("SIGN requires numeric argument".to_string()),
        }
    }

    fn description(&self) -> Option<&'static str> {
        Some("Return -1, 0, or 1 depending on the sign of the argument")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upper_function() {
        let func = UpperFunction;
        let result = func.execute(&[SqlValue::Text("hello".to_string())]);
        assert_eq!(result.unwrap(), SqlValue::Text("HELLO".to_string()));
    }

    #[test]
    fn test_lower_function() {
        let func = LowerFunction;
        let result = func.execute(&[SqlValue::Text("HELLO".to_string())]);
        assert_eq!(result.unwrap(), SqlValue::Text("hello".to_string()));
    }

    #[test]
    fn test_length_function() {
        let func = LengthFunction;
        let result = func.execute(&[SqlValue::Text("hello".to_string())]);
        assert_eq!(result.unwrap(), SqlValue::Integer(5));
    }

    #[test]
    fn test_concat_function() {
        let func = ConcatFunction;
        let result = func.execute(&[
            SqlValue::Text("hello".to_string()),
            SqlValue::Text(" ".to_string()),
            SqlValue::Text("world".to_string()),
        ]);
        assert_eq!(result.unwrap(), SqlValue::Text("hello world".to_string()));
    }

    #[test]
    fn test_abs_function() {
        let func = AbsFunction;
        assert_eq!(
            func.execute(&[SqlValue::Integer(-5)]).unwrap(),
            SqlValue::Integer(5)
        );
        assert_eq!(
            func.execute(&[SqlValue::Real(-2.5)]).unwrap(),
            SqlValue::Real(2.5)
        );
    }

    #[test]
    fn test_sqrt_function() {
        let func = SqrtFunction;
        assert_eq!(
            func.execute(&[SqlValue::Integer(16)]).unwrap(),
            SqlValue::Real(4.0)
        );
        assert!(func.execute(&[SqlValue::Integer(-1)]).is_err());
    }

    #[test]
    fn test_extension_registry() {
        let mut registry = ExtensionRegistry::new();

        // Register string functions
        registry
            .register(Box::new(StringFunctionsExtension))
            .unwrap();

        // Check function exists
        assert!(registry.has_scalar_function("UPPER"));
        assert!(registry.has_scalar_function("upper")); // case insensitive

        // Execute function
        let result = registry.execute_scalar("UPPER", &[SqlValue::Text("test".to_string())]);
        assert_eq!(result.unwrap(), SqlValue::Text("TEST".to_string()));
    }

    #[test]
    fn test_function_signature_validation() {
        let sig = FunctionSignature::new(vec![ArgType::TextLike], DataType::Text(None));

        // Valid
        assert!(sig.validate(&[SqlValue::Text("hello".to_string())]).is_ok());

        // Wrong type
        assert!(sig.validate(&[SqlValue::Integer(42)]).is_err());

        // Wrong count
        assert!(sig.validate(&[]).is_err());
    }
}
