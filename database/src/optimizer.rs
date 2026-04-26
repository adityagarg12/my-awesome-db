use std::collections::{HashMap, HashSet};
use common::query::{
    ComparisionOperator, ComparisionValue, CrossData, FilterData, JoinAlgorithm, JoinData,
    Predicate, ProjectData, QueryOp, SortData, SortSpec,
};
use crate::catalog::Catalog;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn optimize(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let op = bottom_up_pass(op, catalog);
    let op = bottom_up_pass(op, catalog); 
    push_projections(op, None, catalog)   
}

// ---------------------------------------------------------------------------
// Bottom-up pass
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

fn merge_adjacent_filters(op: QueryOp) -> QueryOp {
    let QueryOp::Filter(outer) = op else { return op; };
    if let QueryOp::Filter(inner) = *outer.underlying {
        let merged = QueryOp::Filter(FilterData {
            underlying: inner.underlying,
            predicates: inner.predicates.into_iter().chain(outer.predicates).collect(),
        });
        merge_adjacent_filters(merged) 
    } else {
        QueryOp::Filter(outer)
    }
}

fn push_filter_through_sort(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Filter(d) = op else { return op; };
    if let QueryOp::Sort(sd) = *d.underlying {
        let new_filter = QueryOp::Filter(FilterData {
            underlying: sd.underlying,
            predicates: d.predicates,
        });
        let new_filter = push_selections(new_filter, catalog);
        QueryOp::Sort(SortData {
            underlying: Box::new(new_filter),
            sort_specs: sd.sort_specs,
        })
    } else {
        QueryOp::Filter(d)
    }
}

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

// ---------------------------------------------------------------------------
// Cardinality-Aware Join Tree Builder (Generic)
// ---------------------------------------------------------------------------

fn build_join_tree(inputs: Vec<QueryOp>, predicates: Vec<Predicate>, catalog: &Catalog) -> QueryOp {
    assert!(!inputs.is_empty(), "build_join_tree: empty input list");

    if inputs.len() == 1 {
        let mut v = inputs;
        let input = v.remove(0);
        return if predicates.is_empty() {
            input
        } else {
            QueryOp::Filter(FilterData { underlying: Box::new(input), predicates })
        };
    }

    let col_sets: Vec<HashSet<String>> = inputs.iter()
        .map(|op| derive_columns_set(op, catalog))
        .collect();

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
            let is_equi = matches!(pred.operator, ComparisionOperator::EQ)
                && matches!(pred.value, ComparisionValue::Column(_));
            if is_equi {
                equi_preds.push(pred);
            } else {
                residual.push(pred);
            }
        }
    }

    let mut inputs: Vec<QueryOp> = inputs.into_iter().zip(per_input)
        .map(|(input, preds)| {
            if preds.is_empty() { input } else {
                QueryOp::Filter(FilterData { underlying: Box::new(input), predicates: preds })
            }
        })
        .collect();

    let mut col_sets = col_sets;

    // --- GENERIC DOMAIN NDV ESTIMATOR (Equivalence Classes) ---
    // Uses Union-Find to group columns that share an equi-join predicate,
    // assigning all of them the NDV of the smallest base table in the group.
    let mut uf: HashMap<String, String> = HashMap::new();
    for table in catalog.tables() {
        for col in table.columns() {
            uf.insert(col.name.clone(), col.name.clone());
        }
    }

    fn find(uf: &HashMap<String, String>, s: &str) -> String {
        let mut curr = s.to_string();
        while let Some(parent) = uf.get(&curr) {
            if parent == &curr { break; }
            curr = parent.clone();
        }
        curr
    }

    fn union(uf: &mut HashMap<String, String>, s1: &str, s2: &str) {
        let root1 = find(uf, s1);
        let root2 = find(uf, s2);
        if root1 != root2 {
            uf.insert(root1, root2);
        }
    }

    for pred in &equi_preds {
        if let ComparisionValue::Column(right_col) = &pred.value {
            union(&mut uf, &pred.column_name, right_col);
        }
    }

    let mut root_to_ndv: HashMap<String, f64> = HashMap::new();
    for table in catalog.tables() {
        let t_size = table.num_rows as f64;
        for col in table.columns() {
            let root = find(&uf, &col.name);
            let current_min = root_to_ndv.get(&root).cloned().unwrap_or(t_size);
            root_to_ndv.insert(root, current_min.min(t_size));
        }
    }

    let mut col_to_ndv: HashMap<String, f64> = HashMap::new();
    for table in catalog.tables() {
        for col in table.columns() {
            let root = find(&uf, &col.name);
            let ndv = *root_to_ndv.get(&root).unwrap_or(&100_000.0);
            col_to_ndv.insert(col.name.clone(), ndv.max(1.0));
        }
    }

    let estimate_join_cardinality = |left_size: f64, right_size: f64, applicable_eq: &[Predicate]| -> f64 {
        if applicable_eq.is_empty() {
            return left_size * right_size;
        }
        let mut min_card = f64::MAX;
        for pred in applicable_eq {
            if let ComparisionValue::Column(right_col) = &pred.value {
                let ndv1 = *col_to_ndv.get(&pred.column_name).unwrap_or(&left_size);
                let ndv2 = *col_to_ndv.get(right_col).unwrap_or(&right_size);
                let max_ndv = ndv1.max(ndv2).max(1.0);
                let card = (left_size * right_size) / max_ndv;
                if card < min_card {
                    min_card = card;
                }
            }
        }
        min_card
    };

    // --- Greedy left-deep join tree construction ---
    let start_pos = (0..inputs.len())
        .min_by(|&a, &b| {
            let sa = estimate_op_size(&inputs[a], catalog);
            let sb = estimate_op_size(&inputs[b], catalog);
            sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
        })
        .unwrap_or(0);
    
    let mut current          = inputs.remove(start_pos);
    let mut joined_cols      = col_sets.remove(start_pos);
    let mut rem_equi         = equi_preds;
    let mut rem_residual     = residual;
    let mut current_size     = estimate_op_size(&current, catalog);

    let all_covered = |p: &Predicate, cols: &HashSet<String>| -> bool {
        predicate_columns(p).iter().all(|c| cols.contains(c))
    };

    while !inputs.is_empty() {
        let mut best_pos = 0;
        let mut best_card = f64::MAX;
        let mut best_is_connected = false;

        for (i, op) in inputs.iter().enumerate() {
            let cols = &col_sets[i];
            
            let mut applicable = Vec::new();
            for p in &rem_equi {
                let refs: HashSet<String> = predicate_columns(p).into_iter().collect();
                let touches_joined = refs.iter().any(|c| joined_cols.contains(c));
                let touches_cand = refs.iter().any(|c| cols.contains(c));
                if touches_joined && touches_cand {
                    applicable.push(p.clone());
                }
            }

            let is_connected = !applicable.is_empty();
            let cand_size = estimate_op_size(op, catalog);
            
            let card = if is_connected {
                estimate_join_cardinality(current_size, cand_size, &applicable)
            } else {
                current_size * cand_size // Cross product penalty
            };

            if is_connected && !best_is_connected {
                best_pos = i;
                best_card = card;
                best_is_connected = true;
            } else if is_connected == best_is_connected {
                if card < best_card {
                    best_pos = i;
                    best_card = card;
                }
            }
        }

        let next_pos = best_pos;
        let next      = inputs.remove(next_pos);
        let next_cols = col_sets.remove(next_pos);
        current_size  = best_card; 

        let new_joined: HashSet<String> = joined_cols.iter().cloned()
            .chain(next_cols)
            .collect();

        let (applicable_eq, leftover_eq): (Vec<_>, Vec<_>) = rem_equi
            .into_iter()
            .partition(|p| all_covered(p, &new_joined));
        rem_equi = leftover_eq;

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
                algorithm:  JoinAlgorithm::SortMerge,
            })
        };

        if !applicable_res.is_empty() {
            current = QueryOp::Filter(FilterData {
                underlying: Box::new(current),
                predicates: applicable_res,
            });
        }

        joined_cols = new_joined;
    }

    let stray: Vec<Predicate> = rem_equi.into_iter().chain(rem_residual).collect();
    if !stray.is_empty() {
        current = QueryOp::Filter(FilterData {
            underlying: Box::new(current),
            predicates: stray,
        });
    }

    apply_sort_requirements_recursive(current, catalog)
}

fn apply_sort_requirements_recursive(op: QueryOp, catalog: &Catalog) -> QueryOp {
    match op {
        QueryOp::Join(d) => {
            let left  = apply_sort_requirements_recursive(*d.left,  catalog);
            let right = apply_sort_requirements_recursive(*d.right, catalog);
            let joined = QueryOp::Join(JoinData {
                left:       Box::new(left),
                right:      Box::new(right),
                predicates: d.predicates,
                algorithm:  d.algorithm,
            });
            select_join_algorithm(joined, catalog)
        }
        QueryOp::Filter(d) => QueryOp::Filter(FilterData {
            underlying: Box::new(apply_sort_requirements_recursive(*d.underlying, catalog)),
            predicates: d.predicates,
        }),
        QueryOp::Sort(d) => QueryOp::Sort(SortData {
            underlying: Box::new(apply_sort_requirements_recursive(*d.underlying, catalog)),
            sort_specs:  d.sort_specs,
        }),
        other => other,
    }
}

fn push_selections(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Filter(filter_data) = op else { return op; };

    if !matches!(*filter_data.underlying, QueryOp::Cross(_)) {
        return QueryOp::Filter(filter_data);
    }
    let QueryOp::Cross(cross_data) = *filter_data.underlying else { unreachable!() };

    let mut inputs = flatten_cross_inputs(*cross_data.left);
    inputs.extend(flatten_cross_inputs(*cross_data.right));

    build_join_tree(inputs, filter_data.predicates, catalog)
}

fn is_sorted_on(op: &QueryOp, specs: &[SortSpec]) -> bool {
    match op {
        QueryOp::Sort(d) => {
            d.sort_specs.len() >= specs.len()
                && specs.iter().zip(d.sort_specs.iter()).all(|(req, has)| {
                    req.column_name == has.column_name && req.ascending == has.ascending
                })
        }
        QueryOp::Filter(d) => is_sorted_on(&d.underlying, specs),
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

fn make_sort_specs(key_cols: &[String]) -> Vec<SortSpec> {
    key_cols.iter()
        .map(|col| SortSpec { column_name: col.clone(), ascending: true })
        .collect()
}

fn select_join_algorithm(op: QueryOp, catalog: &Catalog) -> QueryOp {
    let QueryOp::Join(mut d) = op else { return op; };

    let left_set  = derive_columns_set(&d.left,  catalog);
    let right_set = derive_columns_set(&d.right, catalog);

    let mut smj_preds = Vec::new();
    let mut residual_preds = Vec::new();
    let mut left_key  = Vec::new();
    let mut right_key = Vec::new();

    for pred in d.predicates {
        let mut is_smj = false;
        if matches!(pred.operator, ComparisionOperator::EQ) {
            if let ComparisionValue::Column(other) = &pred.value {
                if left_set.contains(&pred.column_name) && right_set.contains(other) {
                    left_key.push(pred.column_name.clone());
                    right_key.push(other.clone());
                    is_smj = true;
                } else if right_set.contains(&pred.column_name) && left_set.contains(other) {
                    left_key.push(other.clone());
                    right_key.push(pred.column_name.clone());
                    is_smj = true;
                }
            }
        }
        
        if is_smj {
            smj_preds.push(pred);
        } else {
            residual_preds.push(pred);
        }
    }

    if smj_preds.is_empty() {
        let mut all_preds = smj_preds;
        all_preds.extend(residual_preds);
        return QueryOp::Filter(FilterData {
            underlying: Box::new(QueryOp::Cross(CrossData {
                left: d.left,
                right: d.right,
            })),
            predicates: all_preds,
        });
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
    d.predicates = smj_preds; 

    let join_op = QueryOp::Join(d);

    if residual_preds.is_empty() {
        join_op
    } else {
        QueryOp::Filter(FilterData {
            underlying: Box::new(join_op),
            predicates: residual_preds,
        })
    }
}

// ---------------------------------------------------------------------------
// Top-down pass: projection pushdown
// ---------------------------------------------------------------------------

pub fn push_projections(op: QueryOp, needed: Option<&HashSet<String>>, catalog: &Catalog) -> QueryOp {
    match op {
        QueryOp::Scan(_) => {
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
// Size estimation
// ---------------------------------------------------------------------------

fn estimate_op_size(op: &QueryOp, catalog: &Catalog) -> f64 {
    match op {
        QueryOp::Scan(d) => {
            catalog.get_table(&d.table_id)
                .map(|t| t.num_rows as f64)
                .unwrap_or(100_000.0)
        }
        QueryOp::Filter(d) => {
            let base = estimate_op_size(&d.underlying, catalog);
            base * 0.1f64.powi(d.predicates.len() as i32)
        }
        QueryOp::Project(d) => estimate_op_size(&d.underlying, catalog),
        QueryOp::Sort(d)    => estimate_op_size(&d.underlying, catalog),
        _ => 100_000.0,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn predicate_columns(pred: &Predicate) -> Vec<String> {
    let mut cols = vec![pred.column_name.clone()];
    if let ComparisionValue::Column(name) = &pred.value {
        cols.push(name.clone());
    }
    cols
}

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

fn derive_columns_set(op: &QueryOp, catalog: &Catalog) -> HashSet<String> {
    derive_columns_ordered(op, catalog).into_iter().collect()
}