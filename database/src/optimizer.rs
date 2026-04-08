use std::collections::HashSet;
use common::query::{
    ComparisionValue, CrossData, FilterData, JoinAlgorithm, JoinData,
    Predicate, ProjectData, QueryOp, SortData, SortSpec,
};
use crate::catalog::Catalog;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Apply all rule-based optimizations and return the rewritten tree.
/// Two bottom-up passes ensure rules composed by earlier rules (e.g. a Sort
/// node added for SMJ that can then be eliminated) are also handled.
pub fn optimize(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let op = bottom_up_pass(op, catalog);
    let op = bottom_up_pass(op, catalog); // second pass catches interactions
    push_projections(op, None, catalog)   // top-down projection pushdown
}

// ---------------------------------------------------------------------------
// Bottom-up pass: apply local rules after recursing into children
// ---------------------------------------------------------------------------

fn bottom_up_pass(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let op = optimize_children(op, catalog);
    let op = merge_adjacent_filters(op);
    let op = push_filter_through_sort(op, catalog);
    let op = push_selections(op, catalog);
    let op = eliminate_redundant_sort(op);
    select_join_algorithm(op, catalog)
}

fn optimize_children(op: QueryOp, catalog: &Catalog) -> QueryOp {
    match op {
        QueryOp::Scan(_) => op,
        QueryOp::Filter(d) => QueryOp::Filter(FilterData {
            underlying:  Box::new(bottom_up_pass(*d.underlying, catalog)),
            predicates:  d.predicates,
        }),
        QueryOp::Project(d) => QueryOp::Project(ProjectData {
            underlying:       Box::new(bottom_up_pass(*d.underlying, catalog)),
            column_name_map:  d.column_name_map,
        }),
        QueryOp::Sort(d) => QueryOp::Sort(SortData {
            underlying:  Box::new(bottom_up_pass(*d.underlying, catalog)),
            sort_specs:  d.sort_specs,
        }),
        QueryOp::Cross(d) => QueryOp::Cross(CrossData {
            left:  Box::new(bottom_up_pass(*d.left,  catalog)),
            right: Box::new(bottom_up_pass(*d.right, catalog)),
        }),
        QueryOp::Join(d) => QueryOp::Join(JoinData {
            left:       Box::new(bottom_up_pass(*d.left,  catalog)),
            right:      Box::new(bottom_up_pass(*d.right, catalog)),
            predicates: d.predicates,
            algorithm:  d.algorithm,
        }),
    }
}

// ---------------------------------------------------------------------------
// Rule: merge adjacent Filter nodes
//
// Filter(Filter(child, inner_preds), outer_preds)
//   → Filter(child, inner_preds ++ outer_preds)
//
// Benefit: fewer operator calls per row; more predicates available for
// subsequent pushdown rules in the same pass.
// ---------------------------------------------------------------------------
fn merge_adjacent_filters(op: QueryOp) -> QueryOp {
    let QueryOp::Filter(outer) = op else { return op; };
    if let QueryOp::Filter(inner) = *outer.underlying {
        let merged = QueryOp::Filter(FilterData {
            underlying: inner.underlying,
            predicates: inner.predicates.into_iter().chain(outer.predicates).collect(),
        });
        merge_adjacent_filters(merged) // recurse: triple/quadruple Filter possible
    } else {
        QueryOp::Filter(outer)
    }
}

// ---------------------------------------------------------------------------
// Rule: push Filter through Sort
//
// Filter(Sort(child, specs), preds) → Sort(Filter(child, preds), specs)
//
// Sort reads ALL rows; filtering first reduces what Sort must process.
// Safe because Sort does not rename columns.
// After rewriting, immediately apply push_selections to the new
// Filter(child) in case child is a Cross (enabling join fusion).
// ---------------------------------------------------------------------------
fn push_filter_through_sort(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Filter(d) = op else { return op; };
    if let QueryOp::Sort(sd) = *d.underlying {
        let new_filter = QueryOp::Filter(FilterData {
            underlying: sd.underlying,
            predicates: d.predicates,
        });
        // Immediately try further pushdown on the new Filter(child)
        let new_filter = push_selections(new_filter, catalog);
        QueryOp::Sort(SortData {
            underlying: Box::new(new_filter),
            sort_specs: sd.sort_specs,
        })
    } else {
        QueryOp::Filter(d)
    }
}

// ---------------------------------------------------------------------------
// Rule: selection pushdown
//
// Filter(Cross(A, B), preds):
//   Classify each predicate:
//     left_preds  — all referenced columns are in A's schema
//     right_preds — all referenced columns are in B's schema
//     join_preds  — straddle both sides
//   Push left_preds → Filter(A, left_preds)
//   Push right_preds → Filter(B, right_preds)
//   join_preds → Join(A, B, join_preds, BNlj)  [algorithm chosen later]
//   (select_join_algorithm will pick SMJ if appropriate)
// ---------------------------------------------------------------------------
fn push_selections(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Filter(filter_data) = op else { return op; };

    let QueryOp::Cross(cross_data) = *filter_data.underlying else {
        return QueryOp::Filter(filter_data);
    };

    let left_cols  = derive_columns_set(&cross_data.left,  catalog);
    let right_cols = derive_columns_set(&cross_data.right, catalog);

    let mut left_preds  = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds  = Vec::new();

    for pred in filter_data.predicates {
        let refs: HashSet<String> = predicate_columns(&pred).into_iter().collect();
        if refs.iter().all(|c| left_cols.contains(c)) {
            left_preds.push(pred);
        } else if refs.iter().all(|c| right_cols.contains(c)) {
            right_preds.push(pred);
        } else {
            join_preds.push(pred);
        }
    }

    let left = if left_preds.is_empty() {
        *cross_data.left
    } else {
        QueryOp::Filter(FilterData { underlying: cross_data.left, predicates: left_preds })
    };
    let right = if right_preds.is_empty() {
        *cross_data.right
    } else {
        QueryOp::Filter(FilterData { underlying: cross_data.right, predicates: right_preds })
    };

    if join_preds.is_empty() {
        QueryOp::Cross(CrossData { left: Box::new(left), right: Box::new(right) })
    } else {
        QueryOp::Join(JoinData {
            left:       Box::new(left),
            right:      Box::new(right),
            predicates: join_preds,
            algorithm:  JoinAlgorithm::BNlj, // overridden by select_join_algorithm
        })
    }
}

// ---------------------------------------------------------------------------
// Rule: eliminate redundant Sort
//
// Sort(child, specs) where child is already sorted on specs → child
//
// "Already sorted on specs" means the subtree root (ignoring Filter
// wrappers, which preserve sort order) is a Sort node whose first
// len(specs) entries match specs exactly.
// ---------------------------------------------------------------------------
fn is_sorted_on(op: &QueryOp, specs: &[SortSpec]) -> bool {
    match op {
        QueryOp::Sort(d) => {
            d.sort_specs.len() >= specs.len()
                && specs.iter().zip(d.sort_specs.iter()).all(|(req, has)| {
                    req.column_name == has.column_name && req.ascending == has.ascending
                })
        }
        // Filter does not reorder rows — look through it
        QueryOp::Filter(d) => is_sorted_on(&d.underlying, specs),
        // Project may remove the sort key column, making the sort order unknowable
        // from the outside; treat as unsorted (conservative).
        _ => false,
    }
}

fn eliminate_redundant_sort(op: QueryOp) -> QueryOp {
    let QueryOp::Sort(d) = op else { return op; };
    if is_sorted_on(&d.underlying, &d.sort_specs) {
        *d.underlying
    } else {
        QueryOp::Sort(d)
    }
}

// ---------------------------------------------------------------------------
// Rule: choose join algorithm — always Sort-Merge Join
//
// For every Join node:
//   1. Determine left and right key column names from equi-join predicates.
//   2. If the left input is not already sorted on its key → wrap with Sort.
//   3. Same for right.
//   4. Set algorithm = SortMerge.
//
// "Always SMJ" policy: the optimizer always prefers SMJ since it avoids the
// disk spill of B-NLJ and is more cache-friendly for large sorted inputs.
// The Sort nodes added here are subject to `eliminate_redundant_sort` on the
// next pass if the input happens to already be sorted.
// ---------------------------------------------------------------------------
fn extract_join_key_cols(
    predicates: &[Predicate],
    left: &QueryOp,
    right: &QueryOp,
    catalog: &Catalog,
) -> (Vec<String>, Vec<String>) {
    let left_set  = derive_columns_set(left,  catalog);
    let right_set = derive_columns_set(right, catalog);

    let mut left_key  = Vec::new();
    let mut right_key = Vec::new();

    for pred in predicates {
        if let ComparisionValue::Column(other) = &pred.value {
            if left_set.contains(&pred.column_name) && right_set.contains(other) {
                left_key.push(pred.column_name.clone());
                right_key.push(other.clone());
            } else if right_set.contains(&pred.column_name) && left_set.contains(other) {
                left_key.push(other.clone());
                right_key.push(pred.column_name.clone());
            }
        }
    }
    (left_key, right_key)
}

fn make_sort_specs(key_cols: &[String]) -> Vec<SortSpec> {
    key_cols.iter()
        .map(|col| SortSpec { column_name: col.clone(), ascending: true })
        .collect()
}

fn select_join_algorithm(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Join(mut d) = op else { return op; };

    let (left_key, right_key) = extract_join_key_cols(&d.predicates, &d.left, &d.right, catalog);

    if left_key.is_empty() {
        // No equi-join keys found — can't use SMJ, keep B-NLJ
        return QueryOp::Join(d);
    }

    let left_specs  = make_sort_specs(&left_key);
    let right_specs = make_sort_specs(&right_key);

    if !is_sorted_on(&d.left, &left_specs) {
        d.left = Box::new(QueryOp::Sort(SortData {
            underlying: d.left,
            sort_specs: left_specs,
        }));
    }
    if !is_sorted_on(&d.right, &right_specs) {
        d.right = Box::new(QueryOp::Sort(SortData {
            underlying: d.right,
            sort_specs: right_specs,
        }));
    }

    d.algorithm = JoinAlgorithm::SortMerge;
    QueryOp::Join(d)
}

// ---------------------------------------------------------------------------
// Top-down pass: projection pushdown
//
// Propagates a `needed` set of column names downward. At each Cross/Join,
// computes what each side actually needs (parent needed ∩ side_cols ∪ pred
// cols from that side) and inserts a narrow Project if that is a strict
// subset of the side's full schema. This reduces row widths before the
// cross/join pairing, shrinking:
//   - In-memory row sizes (more rows fit per chunk / per RunReader block)
//   - Encoded block sizes (more rows per block on disk)
//
// `needed = None` means the parent requires all columns (top-level call).
// ---------------------------------------------------------------------------

pub fn push_projections(op: QueryOp, needed: Option<&HashSet<String>>, catalog: &Catalog) -> QueryOp {
    match op {
        QueryOp::Scan(_) => {
            // Leaf: if parent needs a strict subset, wrap with a narrow Project
            if let Some(n) = needed {
                let all = derive_columns_ordered(&op, catalog);
                let col_map: Vec<(String, String)> = all.iter()
                    .filter(|c| n.contains(*c))
                    .map(|c| (c.clone(), c.clone()))
                    .collect();
                if col_map.len() < all.len() {
                    return QueryOp::Project(ProjectData {
                        underlying: Box::new(op),
                        column_name_map: col_map,
                    });
                }
            }
            op
        }

        QueryOp::Project(d) => {
            // Keep only outputs the parent actually needs; propagate the
            // corresponding "from" columns down to the child.
            let col_map: Vec<(String, String)> = d.column_name_map.into_iter()
                .filter(|(_, to)| needed.map_or(true, |n| n.contains(to)))
                .collect();
            let child_needed: HashSet<String> = col_map.iter()
                .map(|(from, _)| from.clone())
                .collect();
            QueryOp::Project(ProjectData {
                underlying: Box::new(push_projections(*d.underlying, Some(&child_needed), catalog)),
                column_name_map: col_map,
            })
        }

        QueryOp::Filter(d) => {
            let pred_cols: HashSet<String> = d.predicates.iter()
                .flat_map(|p| predicate_columns(p))
                .collect();
            let child_needed: Option<HashSet<String>> = needed.map(|n| {
                n.iter().cloned().chain(pred_cols).collect()
            });
            QueryOp::Filter(FilterData {
                underlying: Box::new(push_projections(*d.underlying, child_needed.as_ref(), catalog)),
                predicates: d.predicates,
            })
        }

        QueryOp::Sort(d) => {
            let spec_cols: HashSet<String> = d.sort_specs.iter()
                .map(|s| s.column_name.clone())
                .collect();
            let child_needed: Option<HashSet<String>> = needed.map(|n| {
                n.iter().cloned().chain(spec_cols).collect()
            });
            QueryOp::Sort(SortData {
                underlying: Box::new(push_projections(*d.underlying, child_needed.as_ref(), catalog)),
                sort_specs: d.sort_specs,
            })
        }

        QueryOp::Cross(d) => {
            let (left, right) = narrow_join_inputs(*d.left, *d.right, &[], needed, catalog);
            QueryOp::Cross(CrossData { left: Box::new(left), right: Box::new(right) })
        }

        QueryOp::Join(d) => {
            let (left, right) = narrow_join_inputs(*d.left, *d.right, &d.predicates, needed, catalog);
            QueryOp::Join(JoinData {
                left: Box::new(left), right: Box::new(right),
                predicates: d.predicates, algorithm: d.algorithm,
            })
        }
    }
}

/// Compute the narrowed needed-column sets for each side of a Cross/Join and
/// recurse into each child with that set.
fn narrow_join_inputs(
    left: QueryOp,
    right: QueryOp,
    predicates: &[Predicate],
    needed: Option<&HashSet<String>>,
    catalog: &Catalog,
) -> (QueryOp, QueryOp) {
    let left_all  = derive_columns_ordered(&left,  catalog);
    let right_all = derive_columns_ordered(&right, catalog);
    let left_set:  HashSet<&String> = left_all.iter().collect();
    let right_set: HashSet<&String> = right_all.iter().collect();

    // Predicate columns required from each side
    let pred_left: HashSet<String> = predicates.iter()
        .flat_map(|p| predicate_columns(p))
        .filter(|c| left_set.contains(c))
        .collect();
    let pred_right: HashSet<String> = predicates.iter()
        .flat_map(|p| predicate_columns(p))
        .filter(|c| right_set.contains(c))
        .collect();

    let left_needed: HashSet<String> = match needed {
        None    => left_all.iter().cloned().collect(),
        Some(n) => n.iter().filter(|c| left_set.contains(c))
                    .cloned().chain(pred_left).collect(),
    };
    let right_needed: HashSet<String> = match needed {
        None    => right_all.iter().cloned().collect(),
        Some(n) => n.iter().filter(|c| right_set.contains(c))
                    .cloned().chain(pred_right).collect(),
    };

    let left  = push_projections(left,  Some(&left_needed),  catalog);
    let right = push_projections(right, Some(&right_needed), catalog);
    (left, right)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Column names referenced by a predicate (LHS and, if RHS is a column, that too).
fn predicate_columns(pred: &Predicate) -> Vec<String> {
    let mut cols = vec![pred.column_name.clone()];
    if let ComparisionValue::Column(name) = &pred.value {
        cols.push(name.clone());
    }
    cols
}

/// Output column names in schema order (for projection insertion).
fn derive_columns_ordered(op: &QueryOp, catalog: &Catalog) -> Vec<String> {
    match op {
        QueryOp::Scan(d) => match catalog.get_table(&d.table_id) {
            Some(t) => t.columns().iter().map(|c| c.name.clone()).collect(),
            None    => vec![],
        },
        QueryOp::Filter(d) => derive_columns_ordered(&d.underlying, catalog),
        QueryOp::Sort(d)   => derive_columns_ordered(&d.underlying, catalog),
        QueryOp::Project(d) => d.column_name_map.iter().map(|(_, to)| to.clone()).collect(),
        QueryOp::Cross(d)  => {
            let mut v = derive_columns_ordered(&d.left, catalog);
            v.extend(derive_columns_ordered(&d.right, catalog));
            v
        }
        QueryOp::Join(d) => {
            let mut v = derive_columns_ordered(&d.left, catalog);
            v.extend(derive_columns_ordered(&d.right, catalog));
            v
        }
    }
}

/// Output column names as a HashSet (for fast membership tests).
fn derive_columns_set(op: &QueryOp, catalog: &Catalog) -> HashSet<String> {
    derive_columns_ordered(op, catalog).into_iter().collect()
}
