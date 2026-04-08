use std::collections::HashSet;
use common::query::{ComparisionValue, CrossData, FilterData, JoinAlgorithm, JoinData, Predicate, ProjectData, QueryOp, SortData};
use crate::catalog::Catalog;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Apply all rule-based optimizations to the query tree and return the
/// rewritten tree. The input and output both use only the five standard
/// QueryOp variants (Scan / Filter / Project / Sort / Cross).
pub fn optimize(op: QueryOp, catalog: &Catalog) -> QueryOp {
    // Rules are applied bottom-up: optimize children first, then the node.
    let op = optimize_children(op, catalog);
    push_selections(op, catalog)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Collect all column names referenced by a predicate (the LHS column and,
/// if the RHS is also a column, that name too).
fn predicate_columns(pred: &Predicate) -> Vec<String> {
    let mut cols = vec![pred.column_name.clone()];
    if let ComparisionValue::Column(name) = &pred.value {
        cols.push(name.clone());
    }
    cols
}

/// Derive the set of output column names produced by `op` without building
/// the physical operator tree. This is a purely structural derivation based
/// on the QueryOp tree and the catalog.
fn derive_columns(op: &QueryOp, catalog: &Catalog) -> HashSet<String> {
    match op {
        QueryOp::Scan(d) => {
            match catalog.get_table(&d.table_id) {
                Some(table) => table.columns().iter().map(|c| c.name.clone()).collect(),
                None        => HashSet::new(),
            }
        }
        // Filter and Sort pass through the child schema unchanged
        QueryOp::Filter(d) => derive_columns(&d.underlying, catalog),
        QueryOp::Sort(d)   => derive_columns(&d.underlying, catalog),
        // Project outputs only the renamed columns (the "to" names)
        QueryOp::Project(d) => {
            d.column_name_map.iter().map(|(_, to)| to.clone()).collect()
        }
        // Cross and Join both concatenate both sides
        QueryOp::Cross(d) => {
            let mut cols = derive_columns(&d.left, catalog);
            cols.extend(derive_columns(&d.right, catalog));
            cols
        }
        QueryOp::Join(d) => {
            let mut cols = derive_columns(&d.left, catalog);
            cols.extend(derive_columns(&d.right, catalog));
            cols
        }
    }
}

// ---------------------------------------------------------------------------
// Rule: selection pushdown
// ---------------------------------------------------------------------------

/// If the current node is `Filter(Cross(A, B), predicates)`, split the
/// predicates into three groups:
///
///   left_preds  — all referenced columns are produced by A
///   right_preds — all referenced columns are produced by B
///   join_preds  — reference columns from both sides (true join predicates)
///
/// Push left_preds below A and right_preds below B. join_preds remain on top
/// of the (now smaller) Cross. This reduces the size of each cross-product
/// input before pairing begins.
///
/// For all other node types the rule is a no-op (children were already
/// optimized by `optimize_children`).
fn push_selections(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Filter(filter_data) = op else {
        return op;
    };

    let QueryOp::Cross(cross_data) = *filter_data.underlying else {
        // Child is not Cross — reconstruct Filter unchanged
        return QueryOp::Filter(filter_data);
    };

    let left_cols  = derive_columns(&cross_data.left,  catalog);
    let right_cols = derive_columns(&cross_data.right, catalog);

    let mut left_preds  = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds  = Vec::new();

    for pred in filter_data.predicates {
        let refs: HashSet<String> = predicate_columns(&pred).into_iter().collect();
        let in_left  = refs.iter().all(|c| left_cols.contains(c));
        let in_right = refs.iter().all(|c| right_cols.contains(c));

        if in_left {
            left_preds.push(pred);
        } else if in_right {
            right_preds.push(pred);
        } else {
            join_preds.push(pred);
        }
    }

    // Wrap each side with its pushed-down predicates (if any)
    let left = if left_preds.is_empty() {
        *cross_data.left
    } else {
        QueryOp::Filter(FilterData {
            underlying: cross_data.left,
            predicates: left_preds,
        })
    };

    let right = if right_preds.is_empty() {
        *cross_data.right
    } else {
        QueryOp::Filter(FilterData {
            underlying: cross_data.right,
            predicates: right_preds,
        })
    };

    // If there are join predicates, emit a Join node (optimizer-internal).
    // If not, emit a plain Cross (pure cartesian product).
    if join_preds.is_empty() {
        QueryOp::Cross(CrossData {
            left:  Box::new(left),
            right: Box::new(right),
        })
    } else {
        QueryOp::Join(JoinData {
            left:       Box::new(left),
            right:      Box::new(right),
            predicates: join_preds,
            algorithm:  JoinAlgorithm::BNlj, // default; planner may override to SortMerge
        })
    }
}

// ---------------------------------------------------------------------------
// Recursive descent: optimize all children before the parent
// ---------------------------------------------------------------------------

fn optimize_children(op: QueryOp, catalog: &Catalog) -> QueryOp {
    match op {
        QueryOp::Scan(_) => op,

        QueryOp::Filter(d) => QueryOp::Filter(FilterData {
            underlying: Box::new(optimize(*d.underlying, catalog)),
            predicates: d.predicates,
        }),

        QueryOp::Project(d) => QueryOp::Project(ProjectData {
            underlying: Box::new(optimize(*d.underlying, catalog)),
            column_name_map: d.column_name_map,
        }),

        QueryOp::Sort(d) => QueryOp::Sort(SortData {
            underlying: Box::new(optimize(*d.underlying, catalog)),
            sort_specs: d.sort_specs,
        }),

        QueryOp::Cross(d) => QueryOp::Cross(CrossData {
            left:  Box::new(optimize(*d.left,  catalog)),
            right: Box::new(optimize(*d.right, catalog)),
        }),

        // Join is optimizer-internal; recurse into its children
        QueryOp::Join(d) => QueryOp::Join(JoinData {
            left:       Box::new(optimize(*d.left,  catalog)),
            right:      Box::new(optimize(*d.right, catalog)),
            predicates: d.predicates,
            algorithm:  d.algorithm,
        }),
    }
}
