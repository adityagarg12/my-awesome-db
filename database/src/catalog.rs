use std::collections::HashMap;

use anyhow::{Result, bail};
use common::{Data, DataType};
use db_config::{DbContext, table::TableSpec};
use db_config::statistics::ColumnStat;

#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    pub name: String,
    pub index: usize,
    pub data_type: DataType,
    /// True when this column's values are in ascending order as rows are read
    /// sequentially from disk. Sourced from `ColumnStat::IsPhysicallyOrdered`
    /// in the catalog. Defaults to `false` when stats are absent — every
    /// optimization that consults this must fall back gracefully so queries
    /// against unannotated tables stay correct.
    pub is_physically_ordered: bool,
    /// True when DensityStat == 1.0 — every value in this column is unique.
    /// This is the catalog's signal that the column is a primary key (or
    /// otherwise unique). Used by `estimate_op_size` to compute exact join
    /// output cardinality (PK-FK joins emit at most one row per FK row).
    /// Defaults to false — fall back to size-only estimates.
    pub is_unique: bool,
    /// `(min, max)` bounds when `RangeStat` is available. Used to compute
    /// per-predicate selectivity in `estimate_op_size::Filter`. None means
    /// fall back to the flat 0.1 selectivity heuristic.
    pub range: Option<(Data, Data)>,
    /// Number of distinct values from `CardinalityStat`. Used as denominator
    /// for equality-predicate selectivity (`col = const` ≈ 1 / distinct).
    pub distinct_count: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TableCatalog {
    pub name: String,
    pub file_id: String,
    pub num_rows: u64,   // max column cardinality — proxy for table row count
    columns: Vec<ColumnCatalog>,
    column_name_to_index: HashMap<String, usize>,
}

impl TableCatalog {
    pub fn get_column(&self, column_name: &str) -> Option<&ColumnCatalog> {
        self.column_name_to_index
            .get(column_name)
            .map(|column_index| &self.columns[*column_index])
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_name_to_index.get(column_name).copied()
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }
}

pub struct Catalog {
    tables: Vec<TableCatalog>,
    table_name_to_index: HashMap<String, usize>,
}

impl Catalog {
    pub fn from_db_context(db_context: &DbContext) -> Result<Self> {
        let mut tables = Vec::new();
        let mut table_name_to_index = HashMap::new();

        for table_spec in db_context.get_table_specs() {
            if table_name_to_index.contains_key(&table_spec.name) {
                bail!("Duplicate table name '{}' in db config", table_spec.name);
            }

            let table_catalog = Self::build_table_catalog(table_spec)?;
            let table_index = tables.len();
            table_name_to_index.insert(table_catalog.name.clone(), table_index);
            tables.push(table_catalog);
        }

        Ok(Self {
            tables,
            table_name_to_index,
        })
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableCatalog> {
        self.table_name_to_index
            .get(table_name)
            .map(|table_index| &self.tables[*table_index])
    }

    pub fn get_file_id(&self, table_name: &str) -> Option<&str> {
        self.get_table(table_name)
            .map(|table_catalog| table_catalog.file_id.as_str())
    }

    pub fn get_column_index(&self, table_name: &str, column_name: &str) -> Option<usize> {
        self.get_table(table_name)
            .and_then(|table_catalog| table_catalog.get_column_index(column_name))
    }

    pub fn get_column_data_type(&self, table_name: &str, column_name: &str) -> Option<&DataType> {
        self.get_table(table_name)
            .and_then(|table_catalog| table_catalog.get_column(column_name))
            .map(|column_catalog| &column_catalog.data_type)
    }

    pub fn tables(&self) -> &[TableCatalog] {
        &self.tables
    }

    fn build_table_catalog(table_spec: &TableSpec) -> Result<TableCatalog> {
        let mut columns = Vec::new();
        let mut column_name_to_index = HashMap::new();

        for (column_index, column_spec) in table_spec.column_specs.iter().enumerate() {
            if column_name_to_index.contains_key(&column_spec.column_name) {
                bail!(
                    "Duplicate column '{}' in table '{}'",
                    column_spec.column_name,
                    table_spec.name
                );
            }

            // Extract per-column stats we know how to consume. Each is
            // None/false when stats are absent, so optimizations stay safe.
            let mut is_physically_ordered = false;
            let mut is_unique = false;
            let mut range: Option<(Data, Data)> = None;
            let mut distinct_count: Option<u64> = None;

            if let Some(stats) = column_spec.stats.as_deref() {
                for stat in stats {
                    match stat {
                        ColumnStat::IsPhysicallyOrdered => { is_physically_ordered = true; }
                        ColumnStat::DensityStat(d) if (d.0 - 1.0).abs() < 1e-6 => {
                            is_unique = true;
                        }
                        ColumnStat::DensityStat(_) => {}
                        ColumnStat::RangeStat(r) => {
                            range = Some((r.lower_bound.clone(), r.upper_bound.clone()));
                        }
                        ColumnStat::CardinalityStat(c) => {
                            distinct_count = Some(c.0);
                        }
                        ColumnStat::HistogramStat(_) => {} // not yet consumed
                    }
                }
            }

            let column_catalog = ColumnCatalog {
                name: column_spec.column_name.clone(),
                index: column_index,
                data_type: column_spec.data_type.clone(),
                is_physically_ordered,
                is_unique,
                range,
                distinct_count,
            };

            column_name_to_index.insert(column_catalog.name.clone(), column_index);
            columns.push(column_catalog);
        }

        let num_rows = table_spec.column_specs.iter()
            .filter_map(|col| col.stats.as_deref())
            .flat_map(|stats| stats.iter())
            .filter_map(|stat| match stat {
                ColumnStat::CardinalityStat(cd) => Some(cd.0),
                _ => None,
            })
            .max()
            .unwrap_or(100_000);

        Ok(TableCatalog {
            name: table_spec.name.clone(),
            file_id: table_spec.file_id.clone(),
            num_rows,
            columns,
            column_name_to_index,
        })
    }
}