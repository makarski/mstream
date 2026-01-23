//! # Rhai Transformation Functions
//!
//! This module provides higher-level transformation functions for ETL workflows.
//! These functions complement Rhai's built-in array methods (`map`, `filter`, `reduce`, etc.)
//! and provide common data transformation patterns.
//!
//! ## Phase 1 Functions
//!
//! ### Aggregation
//! - `sum(array)` / `sum(array, key_fn)` - Sum numeric values
//! - `avg(array)` / `avg(array, key_fn)` - Calculate average
//! - `min(array)` / `min(array, key_fn)` - Find minimum value
//! - `max(array)` / `max(array, key_fn)` - Find maximum value
//!
//! ### Grouping
//! - `group_by(array, key_fn)` - Group elements by key
//! - `count_by(array, key_fn)` - Count occurrences by key
//!
//! ### Array Operations
//! - `unique(array)` / `unique(array, key_fn)` - Remove duplicates
//! - `flatten(array)` - Flatten one level of nested arrays
//!
//! ### Object Operations
//! - `pluck(array, field)` - Extract single field from array of objects
//! - `pick(object, fields)` - Keep only specified fields
//! - `omit(object, fields)` - Remove specified fields

use rhai::{Array, Dynamic, Engine, EvalAltResult, FnPtr, ImmutableString, Map, NativeCallContext};
use std::collections::HashSet;

// =============================================================================
// Helper Types and Functions
// =============================================================================

/// Represents a numeric value that can be either int or float
#[derive(Clone, Copy)]
enum Numeric {
    Int(i64),
    Float(f64),
}

impl Numeric {
    /// Try to extract a numeric value from a Dynamic
    fn from_dynamic(val: &Dynamic) -> Option<Self> {
        val.as_float()
            .ok()
            .map(Numeric::Float)
            .or_else(|| val.as_int().ok().map(Numeric::Int))
    }

    /// Convert to f64 for comparisons
    fn as_f64(self) -> f64 {
        match self {
            Numeric::Int(i) => i as f64,
            Numeric::Float(f) => f,
        }
    }

    /// Convert to Dynamic, preserving original type
    fn to_dynamic(self) -> Dynamic {
        match self {
            Numeric::Int(i) => Dynamic::from_int(i),
            Numeric::Float(f) => Dynamic::from_float(f),
        }
    }
}

/// Accumulator for summing mixed int/float values
struct SumAccumulator {
    int_sum: i64,
    float_sum: f64,
    has_float: bool,
}

impl SumAccumulator {
    fn new() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            has_float: false,
        }
    }

    fn add(&mut self, num: Numeric) {
        match num {
            Numeric::Int(i) => self.int_sum += i,
            Numeric::Float(f) => {
                self.has_float = true;
                self.float_sum += f;
            }
        }
    }

    fn to_dynamic(self) -> Dynamic {
        if self.has_float {
            Dynamic::from_float(self.float_sum + self.int_sum as f64)
        } else {
            Dynamic::from_int(self.int_sum)
        }
    }
}

// =============================================================================
// Aggregation Helpers
// =============================================================================

/// Sum values from an iterator of Dynamics
fn sum_values<'a>(iter: impl Iterator<Item = &'a Dynamic>) -> Dynamic {
    let mut acc = SumAccumulator::new();
    for item in iter {
        if let Some(num) = Numeric::from_dynamic(item) {
            acc.add(num);
        }
    }
    acc.to_dynamic()
}

/// Sum values with a key function
fn sum_with_key(
    ctx: &NativeCallContext,
    arr: &Array,
    key_fn: &FnPtr,
) -> Result<Dynamic, Box<EvalAltResult>> {
    let mut acc = SumAccumulator::new();
    for item in arr {
        let value: Dynamic = key_fn.call_within_context(ctx, (item.clone(),))?;
        if let Some(num) = Numeric::from_dynamic(&value) {
            acc.add(num);
        }
    }
    Ok(acc.to_dynamic())
}

/// Average values from an iterator of Dynamics
fn avg_values<'a>(iter: impl Iterator<Item = &'a Dynamic>) -> Dynamic {
    let (sum, count) = iter.fold(
        (0.0, 0usize),
        |(sum, count), item| match Numeric::from_dynamic(item) {
            Some(num) => (sum + num.as_f64(), count + 1),
            None => (sum, count),
        },
    );

    if count == 0 {
        Dynamic::UNIT
    } else {
        Dynamic::from_float(sum / count as f64)
    }
}

/// Average values with a key function
fn avg_with_key(
    ctx: &NativeCallContext,
    arr: &Array,
    key_fn: &FnPtr,
) -> Result<Dynamic, Box<EvalAltResult>> {
    if arr.is_empty() {
        return Ok(Dynamic::UNIT);
    }

    let mut sum = 0.0;
    let mut count = 0usize;

    for item in arr {
        let value: Dynamic = key_fn.call_within_context(ctx, (item.clone(),))?;
        if let Some(num) = Numeric::from_dynamic(&value) {
            sum += num.as_f64();
            count += 1;
        }
    }

    if count == 0 {
        Ok(Dynamic::UNIT)
    } else {
        Ok(Dynamic::from_float(sum / count as f64))
    }
}

/// Find extreme value (min or max) in array based on comparator
fn find_extreme(arr: &Array, is_better: fn(f64, f64) -> bool) -> Dynamic {
    if arr.is_empty() {
        return Dynamic::UNIT;
    }

    let mut best: Option<Numeric> = None;

    for item in arr {
        if let Some(num) = Numeric::from_dynamic(item) {
            best = Some(match best {
                None => num,
                Some(current) if is_better(num.as_f64(), current.as_f64()) => num,
                Some(current) => current,
            });
        }
    }

    best.map(|n| n.to_dynamic()).unwrap_or(Dynamic::UNIT)
}

/// Find extreme value with key function
fn find_extreme_with_key(
    ctx: &NativeCallContext,
    arr: &Array,
    key_fn: &FnPtr,
    is_better: fn(f64, f64) -> bool,
) -> Result<Dynamic, Box<EvalAltResult>> {
    if arr.is_empty() {
        return Ok(Dynamic::UNIT);
    }

    let mut best_value: Option<Dynamic> = None;
    let mut best_key: Option<f64> = None;

    for item in arr {
        let key: Dynamic = key_fn.call_within_context(ctx, (item.clone(),))?;
        let Some(key_num) = Numeric::from_dynamic(&key) else {
            continue;
        };
        let k = key_num.as_f64();

        if best_key.is_none() || is_better(k, best_key.unwrap()) {
            best_key = Some(k);
            best_value = Some(item.clone());
        }
    }

    Ok(best_value.unwrap_or(Dynamic::UNIT))
}

// =============================================================================
// Grouping Helpers
// =============================================================================

/// Grouping mode determines how items are aggregated by key
enum GroupMode {
    /// Collect items into arrays
    Collect,
    /// Count occurrences
    Count,
}

/// Generic grouping function that handles both group_by and count_by
fn group_items(
    ctx: &NativeCallContext,
    arr: &Array,
    key_fn: &FnPtr,
    mode: GroupMode,
) -> Result<Map, Box<EvalAltResult>> {
    let mut result: Map = Map::new();

    for item in arr {
        let key: Dynamic = key_fn.call_within_context(ctx, (item.clone(),))?;
        let key_str = key.to_string();

        match mode {
            GroupMode::Collect => {
                let group = result
                    .entry(key_str.into())
                    .or_insert_with(|| Dynamic::from(Array::new()));

                if let Some(mut arr) = group.write_lock::<Array>() {
                    arr.push(item.clone());
                }
            }
            GroupMode::Count => {
                let count = result
                    .entry(key_str.into())
                    .or_insert_with(|| Dynamic::from_int(0));

                if let Ok(n) = count.as_int() {
                    *count = Dynamic::from_int(n + 1);
                }
            }
        }
    }

    Ok(result)
}

// =============================================================================
// Registration
// =============================================================================

/// Register all transformation functions with the Rhai engine
pub fn register_transform_functions(engine: &mut Engine) {
    register_aggregations(engine);
    register_grouping(engine);
    register_array_ops(engine);
    register_object_ops(engine);
}

fn register_aggregations(engine: &mut Engine) {
    // sum
    engine.register_fn("sum", |arr: Array| -> Dynamic { sum_values(arr.iter()) });

    engine.register_fn(
        "sum",
        |ctx: NativeCallContext,
         arr: Array,
         key_fn: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> { sum_with_key(&ctx, &arr, &key_fn) },
    );

    // avg
    engine.register_fn("avg", |arr: Array| -> Dynamic {
        if arr.is_empty() {
            Dynamic::UNIT
        } else {
            avg_values(arr.iter())
        }
    });

    engine.register_fn(
        "avg",
        |ctx: NativeCallContext,
         arr: Array,
         key_fn: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> { avg_with_key(&ctx, &arr, &key_fn) },
    );

    // min
    engine.register_fn("min", |arr: Array| -> Dynamic {
        find_extreme(&arr, |a, b| a < b)
    });

    engine.register_fn(
        "min",
        |ctx: NativeCallContext,
         arr: Array,
         key_fn: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            find_extreme_with_key(&ctx, &arr, &key_fn, |a, b| a < b)
        },
    );

    // max
    engine.register_fn("max", |arr: Array| -> Dynamic {
        find_extreme(&arr, |a, b| a > b)
    });

    engine.register_fn(
        "max",
        |ctx: NativeCallContext,
         arr: Array,
         key_fn: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            find_extreme_with_key(&ctx, &arr, &key_fn, |a, b| a > b)
        },
    );
}

fn register_grouping(engine: &mut Engine) {
    engine.register_fn(
        "group_by",
        |ctx: NativeCallContext, arr: Array, key_fn: FnPtr| -> Result<Map, Box<EvalAltResult>> {
            group_items(&ctx, &arr, &key_fn, GroupMode::Collect)
        },
    );

    engine.register_fn(
        "count_by",
        |ctx: NativeCallContext, arr: Array, key_fn: FnPtr| -> Result<Map, Box<EvalAltResult>> {
            group_items(&ctx, &arr, &key_fn, GroupMode::Count)
        },
    );
}

/// Helper for unique with key function - extracted to reduce nesting
fn unique_by_key(
    ctx: &NativeCallContext,
    arr: Array,
    key_fn: &FnPtr,
) -> Result<Array, Box<EvalAltResult>> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut result = Array::new();

    for item in arr {
        let key: Dynamic = key_fn.call_within_context(ctx, (item.clone(),))?;
        let dominated = !seen.insert(format!("{:?}", key));
        if !dominated {
            result.push(item);
        }
    }

    Ok(result)
}

/// Flatten a single item - returns vec of 1 or more items
fn flatten_item(item: Dynamic) -> impl Iterator<Item = Dynamic> {
    item.clone()
        .try_cast::<Array>()
        .map(|arr| arr.into_iter().collect::<Vec<_>>())
        .unwrap_or_else(|| vec![item])
        .into_iter()
}

fn register_array_ops(engine: &mut Engine) {
    // unique(array)
    engine.register_fn("unique", |arr: Array| -> Array {
        let mut seen: HashSet<String> = HashSet::new();
        arr.into_iter()
            .filter(|item| seen.insert(format!("{:?}", item)))
            .collect()
    });

    // unique(array, key_fn)
    engine.register_fn(
        "unique",
        |ctx: NativeCallContext, arr: Array, key_fn: FnPtr| -> Result<Array, Box<EvalAltResult>> {
            unique_by_key(&ctx, arr, &key_fn)
        },
    );

    // flatten(array)
    engine.register_fn("flatten", |arr: Array| -> Array {
        arr.into_iter().flat_map(flatten_item).collect()
    });
}

fn register_object_ops(engine: &mut Engine) {
    // pluck(array, field)
    engine.register_fn("pluck", |arr: Array, field: ImmutableString| -> Array {
        arr.into_iter()
            .map(|item| {
                item.try_cast::<Map>()
                    .and_then(|map| map.get(field.as_str()).cloned())
                    .unwrap_or(Dynamic::UNIT)
            })
            .collect()
    });

    engine.register_fn("pluck", |arr: Array, field: &str| -> Array {
        arr.into_iter()
            .map(|item| {
                item.try_cast::<Map>()
                    .and_then(|map| map.get(field).cloned())
                    .unwrap_or(Dynamic::UNIT)
            })
            .collect()
    });

    // pick(object, fields)
    engine.register_fn("pick", |obj: Map, fields: Array| -> Map {
        fields
            .iter()
            .filter_map(|field| {
                let field_str = field.to_string();
                obj.get(field_str.as_str())
                    .map(|value| (field_str.into(), value.clone()))
            })
            .collect()
    });

    // omit(object, fields)
    engine.register_fn("omit", |obj: Map, fields: Array| -> Map {
        let fields_set: HashSet<String> = fields.iter().map(|f| f.to_string()).collect();

        obj.into_iter()
            .filter(|(key, _)| !fields_set.contains(key.as_str()))
            .collect()
    });
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rhai::Engine;

    fn create_test_engine() -> Engine {
        let mut engine = Engine::new();
        register_transform_functions(&mut engine);
        engine
    }

    mod sum_tests {
        use super::*;

        #[test]
        fn empty_array() {
            let engine = create_test_engine();
            let result: i64 = engine.eval("sum([])").unwrap();
            assert_eq!(result, 0);
        }

        #[test]
        fn integers() {
            let engine = create_test_engine();
            let result: i64 = engine.eval("sum([1, 2, 3, 4, 5])").unwrap();
            assert_eq!(result, 15);
        }

        #[test]
        fn floats() {
            let engine = create_test_engine();
            let result: f64 = engine.eval("sum([1.5, 2.5, 3.0])").unwrap();
            assert!((result - 7.0).abs() < f64::EPSILON);
        }

        #[test]
        fn mixed() {
            let engine = create_test_engine();
            let result: f64 = engine.eval("sum([1, 2.5, 3])").unwrap();
            assert!((result - 6.5).abs() < f64::EPSILON);
        }

        #[test]
        fn with_key_fn() {
            let engine = create_test_engine();
            let result: i64 = engine
                .eval(
                    r#"
                let items = [#{ amount: 10 }, #{ amount: 20 }, #{ amount: 30 }];
                sum(items, |v| v.amount)
            "#,
                )
                .unwrap();
            assert_eq!(result, 60);
        }
    }

    mod avg_tests {
        use super::*;

        #[test]
        fn empty_array() {
            let engine = create_test_engine();
            let result: Dynamic = engine.eval("avg([])").unwrap();
            assert!(result.is_unit());
        }

        #[test]
        fn integers() {
            let engine = create_test_engine();
            let result: f64 = engine.eval("avg([2, 4, 6])").unwrap();
            assert!((result - 4.0).abs() < f64::EPSILON);
        }

        #[test]
        fn with_key_fn() {
            let engine = create_test_engine();
            let result: f64 = engine
                .eval(
                    r#"
                let items = [#{ score: 80 }, #{ score: 90 }, #{ score: 100 }];
                avg(items, |v| v.score)
            "#,
                )
                .unwrap();
            assert!((result - 90.0).abs() < f64::EPSILON);
        }
    }

    mod min_tests {
        use super::*;

        #[test]
        fn empty_array() {
            let engine = create_test_engine();
            let result: Dynamic = engine.eval("min([])").unwrap();
            assert!(result.is_unit());
        }

        #[test]
        fn integers() {
            let engine = create_test_engine();
            let result: i64 = engine.eval("min([5, 2, 8, 1, 9])").unwrap();
            assert_eq!(result, 1);
        }

        #[test]
        fn with_key_fn() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let items = [#{ ts: 300 }, #{ ts: 100 }, #{ ts: 200 }];
                min(items, |v| v.ts)
            "#,
                )
                .unwrap();
            assert_eq!(result.get("ts").unwrap().as_int().unwrap(), 100);
        }
    }

    mod max_tests {
        use super::*;

        #[test]
        fn empty_array() {
            let engine = create_test_engine();
            let result: Dynamic = engine.eval("max([])").unwrap();
            assert!(result.is_unit());
        }

        #[test]
        fn integers() {
            let engine = create_test_engine();
            let result: i64 = engine.eval("max([5, 2, 8, 1, 9])").unwrap();
            assert_eq!(result, 9);
        }

        #[test]
        fn with_key_fn() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let items = [#{ score: 50 }, #{ score: 90 }, #{ score: 70 }];
                max(items, |v| v.score)
            "#,
                )
                .unwrap();
            assert_eq!(result.get("score").unwrap().as_int().unwrap(), 90);
        }
    }

    mod group_by_tests {
        use super::*;

        #[test]
        fn groups_by_key() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let orders = [
                    #{ customer: "alice", amount: 100 },
                    #{ customer: "bob", amount: 200 },
                    #{ customer: "alice", amount: 150 }
                ];
                group_by(orders, |v| v.customer)
            "#,
                )
                .unwrap();

            let alice_orders = result.get("alice").unwrap().clone().cast::<Array>();
            let bob_orders = result.get("bob").unwrap().clone().cast::<Array>();

            assert_eq!(alice_orders.len(), 2);
            assert_eq!(bob_orders.len(), 1);
        }

        #[test]
        fn empty_array() {
            let engine = create_test_engine();
            let result: Map = engine.eval("group_by([], |v| v.key)").unwrap();
            assert!(result.is_empty());
        }
    }

    mod count_by_tests {
        use super::*;

        #[test]
        fn counts_by_key() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let events = [
                    #{ type: "click" },
                    #{ type: "view" },
                    #{ type: "click" },
                    #{ type: "click" }
                ];
                count_by(events, |v| v.type)
            "#,
                )
                .unwrap();

            assert_eq!(result.get("click").unwrap().as_int().unwrap(), 3);
            assert_eq!(result.get("view").unwrap().as_int().unwrap(), 1);
        }

        #[test]
        fn empty_array() {
            let engine = create_test_engine();
            let result: Map = engine.eval("count_by([], |v| v.key)").unwrap();
            assert!(result.is_empty());
        }
    }

    mod unique_tests {
        use super::*;

        #[test]
        fn simple() {
            let engine = create_test_engine();
            let result: Array = engine.eval("unique([1, 2, 1, 3, 2, 4])").unwrap();
            assert_eq!(result.len(), 4);

            let values: Vec<i64> = result.iter().map(|v| v.as_int().unwrap()).collect();
            assert_eq!(values, vec![1, 2, 3, 4]);
        }

        #[test]
        fn strings() {
            let engine = create_test_engine();
            let result: Array = engine.eval(r#"unique(["a", "b", "a", "c", "b"])"#).unwrap();
            assert_eq!(result.len(), 3);
        }

        #[test]
        fn with_key_fn() {
            let engine = create_test_engine();
            let result: Array = engine
                .eval(
                    r#"
                let items = [
                    #{ id: 1, name: "first" },
                    #{ id: 2, name: "second" },
                    #{ id: 1, name: "duplicate" }
                ];
                unique(items, |v| v.id)
            "#,
                )
                .unwrap();
            assert_eq!(result.len(), 2);
        }

        #[test]
        fn empty() {
            let engine = create_test_engine();
            let result: Array = engine.eval("unique([])").unwrap();
            assert!(result.is_empty());
        }
    }

    mod flatten_tests {
        use super::*;

        #[test]
        fn simple() {
            let engine = create_test_engine();
            let result: Array = engine.eval("flatten([[1, 2], [3, 4], [5]])").unwrap();
            assert_eq!(result.len(), 5);

            let values: Vec<i64> = result.iter().map(|v| v.as_int().unwrap()).collect();
            assert_eq!(values, vec![1, 2, 3, 4, 5]);
        }

        #[test]
        fn mixed() {
            let engine = create_test_engine();
            let result: Array = engine.eval("flatten([1, [2, 3], 4])").unwrap();
            assert_eq!(result.len(), 4);
        }

        #[test]
        fn single_level() {
            let engine = create_test_engine();
            let result: Array = engine.eval("flatten([[1, [2, 3]]])").unwrap();
            assert_eq!(result.len(), 2);
            assert!(result[1].is_array());
        }

        #[test]
        fn empty() {
            let engine = create_test_engine();
            let result: Array = engine.eval("flatten([])").unwrap();
            assert!(result.is_empty());
        }
    }

    mod pluck_tests {
        use super::*;

        #[test]
        fn simple() {
            let engine = create_test_engine();
            let result: Array = engine
                .eval(
                    r#"
                let users = [
                    #{ name: "Alice", age: 30 },
                    #{ name: "Bob", age: 25 }
                ];
                pluck(users, "name")
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 2);
            assert_eq!(result[0].clone().into_string().unwrap(), "Alice");
            assert_eq!(result[1].clone().into_string().unwrap(), "Bob");
        }

        #[test]
        fn missing_field() {
            let engine = create_test_engine();
            let result: Array = engine
                .eval(
                    r#"
                let items = [#{ a: 1 }, #{ b: 2 }];
                pluck(items, "a")
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 2);
            assert_eq!(result[0].as_int().unwrap(), 1);
            assert!(result[1].is_unit());
        }

        #[test]
        fn empty() {
            let engine = create_test_engine();
            let result: Array = engine.eval(r#"pluck([], "field")"#).unwrap();
            assert!(result.is_empty());
        }
    }

    mod pick_tests {
        use super::*;

        #[test]
        fn simple() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let user = #{ name: "Alice", password: "secret", email: "a@b.com" };
                pick(user, ["name", "email"])
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 2);
            assert!(result.contains_key("name"));
            assert!(result.contains_key("email"));
            assert!(!result.contains_key("password"));
        }

        #[test]
        fn missing_fields() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let obj = #{ a: 1 };
                pick(obj, ["a", "b", "c"])
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 1);
            assert!(result.contains_key("a"));
        }

        #[test]
        fn empty_fields() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let obj = #{ a: 1, b: 2 };
                pick(obj, [])
            "#,
                )
                .unwrap();

            assert!(result.is_empty());
        }
    }

    mod omit_tests {
        use super::*;

        #[test]
        fn simple() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let user = #{ name: "Alice", password: "secret", email: "a@b.com" };
                omit(user, ["password"])
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 2);
            assert!(result.contains_key("name"));
            assert!(result.contains_key("email"));
            assert!(!result.contains_key("password"));
        }

        #[test]
        fn missing_fields() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let obj = #{ a: 1, b: 2 };
                omit(obj, ["c", "d"])
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 2);
        }

        #[test]
        fn all_fields() {
            let engine = create_test_engine();
            let result: Map = engine
                .eval(
                    r#"
                let obj = #{ a: 1, b: 2 };
                omit(obj, ["a", "b"])
            "#,
                )
                .unwrap();

            assert!(result.is_empty());
        }
    }

    mod integration_tests {
        use super::*;

        #[test]
        fn chained_operations() {
            let engine = create_test_engine();
            let result: i64 = engine
                .eval(
                    r#"
                let orders = [
                    #{ customer: "alice", amount: 100 },
                    #{ customer: "bob", amount: 200 },
                    #{ customer: "alice", amount: 150 }
                ];
                let grouped = group_by(orders, |v| v.customer);
                let customers = grouped.keys();
                customers.len()
            "#,
                )
                .unwrap();

            assert_eq!(result, 2);
        }

        #[test]
        fn flatten_and_unique() {
            let engine = create_test_engine();
            let result: Array = engine
                .eval(
                    r#"
                let docs = [
                    #{ tags: ["a", "b"] },
                    #{ tags: ["b", "c"] }
                ];
                let all_tags = flatten(docs.map(|d| d.tags));
                unique(all_tags)
            "#,
                )
                .unwrap();

            assert_eq!(result.len(), 3);
        }

        #[test]
        fn sum_after_pluck() {
            let engine = create_test_engine();
            let result: i64 = engine
                .eval(
                    r#"
                let items = [
                    #{ qty: 5, price: 10 },
                    #{ qty: 3, price: 20 },
                    #{ qty: 2, price: 15 }
                ];
                sum(pluck(items, "qty"))
            "#,
                )
                .unwrap();

            assert_eq!(result, 10);
        }
    }
}
