use std::collections::HashSet;
use common::{Data, query::{
    ComparisionOperator, ComparisionValue, CrossData, FilterData, JoinAlgorithm, JoinData,
    Predicate, ProjectData, QueryOp, SortData, SortSpec,
}};
use db_config::statistics::ColumnStat;
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
    let op = eliminate_redundant_sort(op, catalog);
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
// Rule: selection pushdown (multi-way join)
//
// Filter(Cross(…nested crosses…), preds):
//   1. Flatten ALL nested Cross nodes into a flat list of inputs.
//   2. Classify each predicate:
//        single-table — every referenced column belongs to exactly one input
//                       → pushed as Filter directly onto that input
//        equi-join    — EQ, column=column, spans exactly two inputs
//                       → goes into the Join node; eligible for SMJ
//        residual     — non-EQ or literal-valued multi-table predicate
//                       → applied as a Filter on top of the join output
//   3. Build a left-deep join tree greedily:
//        At each step pick the next unjoined input that shares an equi-join
//        predicate with the current joined set (fall back to next in list
//        when no equi-join bridge exists, emitting a Cross node).
//        Emit a Join node for equi-join predicates; Cross otherwise.
//        Wrap each step's output with a Filter for any residual predicates
//        that become applicable at that step.
//   4. All Join nodes are upgraded to SMJ by select_join_algorithm.
//
// Handles N-way joins in a single pass, eliminating the per-pass peeling
// limitation of the previous two-table approach.
// ---------------------------------------------------------------------------

/// Recursively collect all Cross leaves into a flat Vec.
/// Non-Cross nodes are left intact as opaque sub-trees.
fn flatten_cross_inputs(op: QueryOp) -> Vec<QueryOp> {
    match op {
        QueryOp::Cross(d) => {
            let mut v = flatten_cross_inputs(*d.left);
            v.extend(flatten_cross_inputs(*d.right));
            v
        }
        other => vec![other],
    }
}

/// Build a left-deep join/cross tree from a flat list of inputs and a predicate set.
///
/// Single-table predicates are pushed directly onto the matching input.
/// Multi-table predicates become join predicates and are applied as soon as
/// both sides are present in the growing joined set.
fn build_join_tree(inputs: Vec<QueryOp>, predicates: Vec<Predicate>, catalog: &Catalog) -> QueryOp {
    assert!(!inputs.is_empty(), "build_join_tree: empty input list");

    // Base case: single input — wrap with any remaining predicates and return.
    if inputs.len() == 1 {
        let mut v = inputs;
        let input = v.remove(0);
        return if predicates.is_empty() {
            input
        } else {
            QueryOp::Filter(FilterData { underlying: Box::new(input), predicates })
        };
    }

    // Column sets per input for predicate classification and greedy search.
    // Computed once from the original (unfiltered) ops; Filter wrapping below
    // does not change the column set.
    let col_sets: Vec<HashSet<String>> = inputs.iter()
        .map(|op| derive_columns_set(op, catalog))
        .collect();

    // Classify predicates into three buckets:
    //   per_input   — single-table: pushed down as Filter on that input
    //   equi_preds  — EQ column=column spanning two inputs: used as Join predicate (SMJ-eligible)
    //   residual    — non-EQ or literal multi-table: applied as Filter on top of the join
    let mut per_input:  Vec<Vec<Predicate>> = vec![Vec::new(); inputs.len()];
    let mut equi_preds: Vec<Predicate>      = Vec::new();
    let mut residual:   Vec<Predicate>      = Vec::new();

    for pred in predicates {
        let refs: HashSet<String> = predicate_columns(&pred).into_iter().collect();
        let touching: Vec<usize>  = (0..col_sets.len())
            .filter(|&i| refs.iter().any(|c| col_sets[i].contains(c)))
            .collect();
        if touching.len() == 1 {
            per_input[touching[0]].push(pred);
        } else {
            // Multi-table: equi-join (EQ + column=column) vs residual
            let is_equi = matches!(pred.operator, ComparisionOperator::EQ)
                && matches!(pred.value, ComparisionValue::Column(_));
            if is_equi {
                equi_preds.push(pred);
            } else {
                residual.push(pred);
            }
        }
    }

    // Push single-table predicates into their respective inputs.
    let mut inputs: Vec<QueryOp> = inputs.into_iter().zip(per_input)
        .map(|(input, preds)| {
            if preds.is_empty() {
                input
            } else {
                QueryOp::Filter(FilterData { underlying: Box::new(input), predicates: preds })
            }
        })
        .collect();

    // col_sets is still valid — Filter wrapping doesn't add/remove columns.
    let mut col_sets = col_sets;

    // --- Sort inputs by estimated cardinality (smallest first) ---
    // Pre-sorting biases the greedy construction toward:
    //   • smaller tables on the right side of B-NLJ (cheaper inner loop)
    //   • smaller intermediate results early in the left-deep tree
    // When cardinality is unknown for an input it sorts to the end (u64::MAX).
    {
        let cards: Vec<u64> = inputs.iter()
            .map(|op| estimate_cardinality(op, catalog).unwrap_or(u64::MAX))
            .collect();
        let mut order: Vec<usize> = (0..inputs.len()).collect();
        order.sort_by_key(|&i| cards[i]);
        // Reorder both inputs and col_sets in lockstep.
        let sorted: Vec<(QueryOp, HashSet<String>)> = order.iter()
            .map(|&i| (inputs[i].clone(), col_sets[i].clone()))
            .collect();
        // Reconstruct (inputs already consumed via clone; replace in-place).
        let (si, sc): (Vec<_>, Vec<_>) = sorted.into_iter().unzip();
        inputs   = si;
        col_sets = sc;
    }

    // --- Greedy left-deep join tree construction ---
    let mut current          = inputs.remove(0);
    let mut joined_cols      = col_sets.remove(0);
    let mut rem_equi         = equi_preds;
    let mut rem_residual     = residual;

    // Helper closure: true when all columns referenced by a predicate are
    // already present in `cols`.
    let all_covered = |p: &Predicate, cols: &HashSet<String>| -> bool {
        predicate_columns(p).iter().all(|c| cols.contains(c))
    };

    while !inputs.is_empty() {
        // Among inputs that bridge to the current joined set via an equi-join
        // predicate, pick the one with the smallest estimated cardinality.
        // This minimises the size of the next intermediate result.
        // Fall back to the smallest-cardinality input when no bridge exists
        // (unavoidable cross product).
        let cards: Vec<u64> = inputs.iter()
            .map(|op| estimate_cardinality(op, catalog).unwrap_or(u64::MAX))
            .collect();

        let bridged: Vec<usize> = (0..inputs.len())
            .filter(|&i| {
                rem_equi.iter().any(|p| {
                    let refs: HashSet<String> = predicate_columns(p).into_iter().collect();
                    refs.iter().any(|c| joined_cols.contains(c))
                        && refs.iter().any(|c| col_sets[i].contains(c))
                })
            })
            .collect();

        let next_pos = if !bridged.is_empty() {
            *bridged.iter().min_by_key(|&&i| cards[i]).unwrap()
        } else {
            (0..inputs.len()).min_by_key(|&i| cards[i]).unwrap_or(0)
        };

        let next      = inputs.remove(next_pos);
        let next_cols = col_sets.remove(next_pos);

        let new_joined: HashSet<String> = joined_cols.iter().cloned()
            .chain(next_cols)
            .collect();

        // Equi-join predicates now fully covered → go into the Join node.
        let (applicable_eq, leftover_eq): (Vec<_>, Vec<_>) = rem_equi
            .into_iter()
            .partition(|p| all_covered(p, &new_joined));
        rem_equi = leftover_eq;

        // Residual predicates now fully covered → Filter on top of the join.
        let (applicable_res, leftover_res): (Vec<_>, Vec<_>) = rem_residual
            .into_iter()
            .partition(|p| all_covered(p, &new_joined));
        rem_residual = leftover_res;

        current = if applicable_eq.is_empty() {
            QueryOp::Cross(CrossData {
                left:  Box::new(current),
                right: Box::new(next),
            })
        } else {
            QueryOp::Join(JoinData {
                left:       Box::new(current),
                right:      Box::new(next),
                predicates: applicable_eq,
                algorithm:  JoinAlgorithm::BNlj, // upgraded to SMJ by select_join_algorithm
            })
        };

        // Apply residual (non-EQ) predicates as a Filter on top.
        if !applicable_res.is_empty() {
            current = QueryOp::Filter(FilterData {
                underlying: Box::new(current),
                predicates: applicable_res,
            });
        }

        joined_cols = new_joined;
    }

    // Stray predicates (disconnected join graph — rare but safe to handle).
    let stray: Vec<Predicate> = rem_equi.into_iter().chain(rem_residual).collect();
    if !stray.is_empty() {
        current = QueryOp::Filter(FilterData {
            underlying: Box::new(current),
            predicates: stray,
        });
    }

    current
}

fn push_selections(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Filter(filter_data) = op else { return op; };

    // Only act when the immediate child is a Cross node.
    if !matches!(*filter_data.underlying, QueryOp::Cross(_)) {
        return QueryOp::Filter(filter_data);
    }
    let QueryOp::Cross(cross_data) = *filter_data.underlying else { unreachable!() };

    // Flatten ALL nested Cross nodes, then build the join tree in one pass.
    let mut inputs = flatten_cross_inputs(*cross_data.left);
    inputs.extend(flatten_cross_inputs(*cross_data.right));

    build_join_tree(inputs, filter_data.predicates, catalog)
}

// ---------------------------------------------------------------------------
// Rule: eliminate redundant Sort  (stat-aware)
//
// Sort(child, specs) where child is already sorted on specs → child
//
// Two ways the child can be "already sorted":
//   1. Structural: child has a Sort node with a compatible sort order.
//   2. Statistical: child is a Scan (possibly under Filters) and every sort
//      column has the `IsPhysicallyOrdered` stat — meaning rows arrive in
//      ascending order of that column when the table is scanned sequentially.
//      This only applies for a single sort spec (composite physical ordering
//      cannot be inferred from per-column stats alone).
// ---------------------------------------------------------------------------
fn is_sorted_on(op: &QueryOp, specs: &[SortSpec], catalog: &Catalog) -> bool {
    match op {
        QueryOp::Sort(d) => {
            d.sort_specs.len() >= specs.len()
                && specs.iter().zip(d.sort_specs.iter()).all(|(req, has)| {
                    req.column_name == has.column_name && req.ascending == has.ascending
                })
        }
        // Filter does not reorder rows — look through it transparently.
        QueryOp::Filter(d) => is_sorted_on(&d.underlying, specs, catalog),
        // A Scan is "sorted" when there is a single ascending sort spec and
        // the column is flagged as physically ordered on disk.
        QueryOp::Scan(d) => {
            specs.len() == 1
                && specs[0].ascending
                && catalog.is_column_physically_ordered(&d.table_id, &specs[0].column_name)
        }
        // Project may drop the sort key — treat conservatively.
        _ => false,
    }
}

fn eliminate_redundant_sort(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Sort(d) = op else { return op; };
    if is_sorted_on(&d.underlying, &d.sort_specs, catalog) {
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
        // Only EQ column=column predicates are valid SMJ sort keys.
        if !matches!(pred.operator, ComparisionOperator::EQ) { continue; }
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

// ---------------------------------------------------------------------------
// Rule: choose join algorithm  (stat-aware)
//
// Decision tree (given equi-join keys exist):
//
//   1. Both sides already sorted on their join key
//      → SMJ, no Sort nodes needed (free merge).
//
//   2. Both sides have estimated cardinality ≤ BNLJ_THRESHOLD
//      → B-NLJ: avoid the external-sort cost entirely.
//        At small sizes, BNLJ fits in memory and is faster than sorting.
//
//   3. Default
//      → SMJ: wrap unsorted inputs in Sort nodes.
//        Better than BNLJ for large relations (avoids repeated re-reads).
//
// When no equi-join keys are found, we cannot use SMJ → keep B-NLJ.
// ---------------------------------------------------------------------------
fn select_join_algorithm(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Join(mut d) = op else { return op; };

    let (left_key, right_key) = extract_join_key_cols(&d.predicates, &d.left, &d.right, catalog);

    if left_key.is_empty() {
        // No equi-join keys found — SMJ not applicable, keep B-NLJ.
        return QueryOp::Join(d);
    }

    let left_specs  = make_sort_specs(&left_key);
    let right_specs = make_sort_specs(&right_key);

    let left_sorted  = is_sorted_on(&d.left,  &left_specs,  catalog);
    let right_sorted = is_sorted_on(&d.right, &right_specs, catalog);

    // Case 1: both inputs already sorted → free SMJ, no Sort nodes.
    if left_sorted && right_sorted {
        d.algorithm = JoinAlgorithm::SortMerge;
        return QueryOp::Join(d);
    }

    // Case 2: both inputs small → B-NLJ avoids external sort overhead.
    // Threshold: if either side exceeds this row count, sort cost is justified.
    const BNLJ_THRESHOLD: u64 = 1_000;
    let left_card  = estimate_cardinality(&d.left,  catalog).unwrap_or(u64::MAX);
    let right_card = estimate_cardinality(&d.right, catalog).unwrap_or(u64::MAX);

    if left_card <= BNLJ_THRESHOLD && right_card <= BNLJ_THRESHOLD {
        d.algorithm = JoinAlgorithm::BNlj;
        return QueryOp::Join(d);
    }

    // Case 3: general — SMJ, wrapping unsorted inputs with Sort.
    if !left_sorted {
        d.left = Box::new(QueryOp::Sort(SortData {
            underlying: d.left,
            sort_specs: left_specs,
        }));
    }
    if !right_sorted {
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

// ---------------------------------------------------------------------------
// Statistics-based cardinality estimation
// ---------------------------------------------------------------------------

/// Walk up through Filter/Sort/Project to find the underlying Scan table id,
/// if the subtree has a single source table. Returns None for joins/crosses.
fn find_source_scan(op: &QueryOp) -> Option<&str> {
    match op {
        QueryOp::Scan(d)    => Some(&d.table_id),
        QueryOp::Filter(d)  => find_source_scan(&d.underlying),
        QueryOp::Sort(d)    => find_source_scan(&d.underlying),
        QueryOp::Project(d) => find_source_scan(&d.underlying),
        _                   => None, // Cross / Join: multiple sources
    }
}

/// Estimate the number of rows produced by `op`.
/// Returns `None` when no cardinality stats are available anywhere in the subtree.
fn estimate_cardinality(op: &QueryOp, catalog: &Catalog) -> Option<u64> {
    match op {
        QueryOp::Scan(d) => catalog.get_table_cardinality(&d.table_id),

        QueryOp::Filter(d) => {
            let child = estimate_cardinality(&d.underlying, catalog)? as f64;
            let table = find_source_scan(&d.underlying);
            let sel   = conjunctive_selectivity(&d.predicates, table, catalog);
            Some((child * sel).ceil() as u64).map(|v| v.max(1))
        }

        QueryOp::Sort(d)    => estimate_cardinality(&d.underlying, catalog),
        QueryOp::Project(d) => estimate_cardinality(&d.underlying, catalog),

        QueryOp::Cross(d) => {
            let l = estimate_cardinality(&d.left,  catalog)? as f64;
            let r = estimate_cardinality(&d.right, catalog)? as f64;
            Some((l * r) as u64)
        }

        QueryOp::Join(d) => {
            let l = estimate_cardinality(&d.left,  catalog)? as f64;
            let r = estimate_cardinality(&d.right, catalog)? as f64;
            let sel = join_selectivity(&d.predicates, &d.left, &d.right, catalog);
            Some((l * r * sel).ceil() as u64).map(|v| v.max(1))
        }
    }
}

/// Combined selectivity of an AND of predicates (independence assumption).
fn conjunctive_selectivity(
    predicates: &[Predicate],
    table_id: Option<&str>,
    catalog: &Catalog,
) -> f64 {
    predicates.iter()
        .map(|p| predicate_selectivity(p, table_id, catalog))
        .product::<f64>()
        .max(1e-6)
}

/// Selectivity of a single predicate against a literal value.
/// Falls back to operator-based defaults when no stats are available.
fn predicate_selectivity(pred: &Predicate, table_id: Option<&str>, catalog: &Catalog) -> f64 {
    // Column-vs-column predicates: can't estimate without join info.
    if matches!(pred.value, ComparisionValue::Column(_)) {
        return match pred.operator {
            ComparisionOperator::EQ => 0.1,
            ComparisionOperator::NE => 0.9,
            _                       => 0.33,
        };
    }

    let (table, col) = match table_id {
        Some(t) => (t, pred.column_name.as_str()),
        None    => return operator_default_sel(&pred.operator),
    };

    // Convert the literal RHS to a comparable Data value.
    let rhs: Data = match &pred.value {
        ComparisionValue::I32(v)    => Data::Int32(*v),
        ComparisionValue::I64(v)    => Data::Int64(*v),
        ComparisionValue::F32(v)    => Data::Float32(*v),
        ComparisionValue::F64(v)    => Data::Float64(*v),
        ComparisionValue::String(v) => Data::String(v.clone()),
        ComparisionValue::Column(_) => unreachable!(),
    };

    for stat in catalog.get_column_stats(table, col) {
        match stat {
            // DensityStat = NDV/total: ideal for equality selectivity.
            ColumnStat::DensityStat(d) => {
                let density = d.0 as f64;
                return match pred.operator {
                    ComparisionOperator::EQ => density,
                    ComparisionOperator::NE => 1.0 - density,
                    // Range: assume uniform → half the values pass on average.
                    _ => 0.5,
                };
            }

            // HistogramStat: count matching bucket row-fraction.
            ColumnStat::HistogramStat(h) => {
                let total: u64 = h.frequency_points.iter().map(|(_, f)| f.0).sum();
                if total == 0 { continue; }

                let matching: u64 = h.frequency_points.iter()
                    .filter(|(range, _)| {
                        bucket_matches_predicate(&pred.operator, &rhs, &range.lower_bound, &range.upper_bound)
                    })
                    .map(|(_, f)| f.0)
                    .sum();

                return matching as f64 / total as f64;
            }

            // RangeStat: for range predicates, estimate covered fraction.
            ColumnStat::RangeStat(r) => {
                if let Some(sel) = range_stat_selectivity(&pred.operator, &rhs, &r.lower_bound, &r.upper_bound) {
                    return sel;
                }
            }

            _ => {}
        }
    }

    operator_default_sel(&pred.operator)
}

/// Default selectivity when no stats exist for a given operator type.
fn operator_default_sel(op: &ComparisionOperator) -> f64 {
    match op {
        ComparisionOperator::EQ  => 0.1,
        ComparisionOperator::NE  => 0.9,
        ComparisionOperator::GT
        | ComparisionOperator::GTE
        | ComparisionOperator::LT
        | ComparisionOperator::LTE => 0.33,
    }
}

/// True when the histogram bucket [lo, hi] contains rows that pass the predicate `op rhs`.
fn bucket_matches_predicate(op: &ComparisionOperator, rhs: &Data, lo: &Data, hi: &Data) -> bool {
    match op {
        // EQ: rhs must fall inside [lo, hi]
        ComparisionOperator::EQ => {
            rhs.partial_cmp(lo).map_or(false, |o| o.is_ge())
                && rhs.partial_cmp(hi).map_or(false, |o| o.is_le())
        }
        // NE: bucket has at least one value ≠ rhs
        ComparisionOperator::NE => true,
        // GT: hi > rhs  (bucket upper end is above the threshold)
        ComparisionOperator::GT => hi.partial_cmp(rhs).map_or(false, |o| o.is_gt()),
        // GTE: hi >= rhs
        ComparisionOperator::GTE => hi.partial_cmp(rhs).map_or(false, |o| o.is_ge()),
        // LT: lo < rhs
        ComparisionOperator::LT => lo.partial_cmp(rhs).map_or(false, |o| o.is_lt()),
        // LTE: lo <= rhs
        ComparisionOperator::LTE => lo.partial_cmp(rhs).map_or(false, |o| o.is_le()),
    }
}

/// Estimate selectivity from a RangeStat using linear interpolation.
/// Returns None when the types are incomparable (e.g., mixed numeric kinds).
fn range_stat_selectivity(op: &ComparisionOperator, rhs: &Data, lo: &Data, hi: &Data) -> Option<f64> {
    // Only meaningful for range operators on the same numeric type.
    let lo_f = data_to_f64(lo)?;
    let hi_f = data_to_f64(hi)?;
    let rhs_f = data_to_f64(rhs)?;
    let span = hi_f - lo_f;
    if span <= 0.0 { return None; }

    Some(match op {
        ComparisionOperator::EQ  => 1.0 / span.max(1.0), // uniform: one value out of span
        ComparisionOperator::NE  => 1.0 - 1.0 / span.max(1.0),
        ComparisionOperator::GT  => ((hi_f - rhs_f) / span).clamp(0.0, 1.0),
        ComparisionOperator::GTE => ((hi_f - rhs_f + 1.0) / span).clamp(0.0, 1.0),
        ComparisionOperator::LT  => ((rhs_f - lo_f) / span).clamp(0.0, 1.0),
        ComparisionOperator::LTE => ((rhs_f - lo_f + 1.0) / span).clamp(0.0, 1.0),
    })
}

fn data_to_f64(d: &Data) -> Option<f64> {
    match d {
        Data::Int32(v)  => Some(*v as f64),
        Data::Int64(v)  => Some(*v as f64),
        Data::Float32(v) => Some(*v as f64),
        Data::Float64(v) => Some(*v),
        Data::String(_) => None,
    }
}

/// Selectivity of an equi-join: 1 / max(NDV_left, NDV_right).
/// Falls back to 0.1 when density stats are absent.
fn join_selectivity(
    predicates: &[Predicate],
    left: &QueryOp,
    right: &QueryOp,
    catalog: &Catalog,
) -> f64 {
    let left_table  = find_source_scan(left);
    let right_table = find_source_scan(right);
    let mut sel = 1.0f64;

    for pred in predicates {
        if !matches!(pred.operator, ComparisionOperator::EQ) { continue; }
        let ComparisionValue::Column(right_col) = &pred.value else { continue; };

        let left_density = left_table
            .and_then(|t| catalog.get_column_density(t, &pred.column_name));
        let right_density = right_table
            .and_then(|t| catalog.get_column_density(t, right_col));

        // sel per predicate = density (higher density → more unique values → lower join sel)
        let pred_sel = match (left_density, right_density) {
            (Some(ld), Some(rd)) => ld.min(rd),
            (Some(d), _) | (_, Some(d)) => d,
            _ => 0.1,
        };
        sel *= pred_sel;
    }
    sel.max(1e-6)
}
