use anyhow::{Result, bail, Context};
use common::{Data, DataType};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<Data>,
}

impl Row {
    pub fn new(values: Vec<Data>) -> Self {
        Self { values }
    }

    pub fn from_values_with_schema_validation(
        values: Vec<Data>,
        schema: &[ColumnCatalog],
    ) -> Result<Self> {
        if values.len() != schema.len() {
            bail!(
                "Row has {} values but schema expects {}",
                values.len(),
                schema.len()
            );
        }

        for (value, column) in values.iter().zip(schema.iter()) {
            let data_type = value_data_type(value);
            if data_type != column.data_type {
                bail!(
                    "Column '{}' expects {:?} but got {:?}",
                    column.name,
                    column.data_type,
                    data_type
                );
            }
        }

        Ok(Self { values })
    }

    pub fn get(&self, column_index: usize) -> Option<&Data> {
        self.values.get(column_index)
    }

    pub fn values(&self) -> &[Data] {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut [Data] {
        &mut self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Approximate heap-backed memory held by this row instance.
    ///
    /// Includes the `Vec<Data>` backing storage and string payload capacities.
    /// Used for conservative operator buffering decisions.
    pub fn exact_mem_size(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>();
        bytes += self.values.capacity() * std::mem::size_of::<Data>();
        for v in &self.values {
            if let Data::String(s) = v {
                bytes += s.capacity();
            }
        }
        bytes
    }
}

pub fn decode_row(
    bytes: &[u8],
    offset: &mut usize,
    schema: &[ColumnCatalog],
) -> Result<Row> {
    let mut values = Vec::new();

    for column in schema {
        let (value, bytes_read) = decode_value(bytes, *offset, &column.data_type)
            .context(format!("Failed to decode column '{}'", column.name))?;
        *offset += bytes_read;
        values.push(value);
    }

    Ok(Row { values })
}

/// Skip one encoded value of `data_type` at `*offset` without decoding or allocating.
pub fn skip_value_at(bytes: &[u8], offset: &mut usize, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Int32 | DataType::Float32 => {
            if *offset + 4 > bytes.len() { bail!("Not enough bytes to skip field"); }
            *offset += 4;
            Ok(())
        }
        DataType::Int64 | DataType::Float64 => {
            if *offset + 8 > bytes.len() { bail!("Not enough bytes to skip field"); }
            *offset += 8;
            Ok(())
        }
        DataType::String => {
            while *offset < bytes.len() && bytes[*offset] != 0 { *offset += 1; }
            if *offset >= bytes.len() { bail!("String missing null terminator during skip"); }
            *offset += 1;
            Ok(())
        }
    }
}

pub fn decode_value(bytes: &[u8], offset: usize, data_type: &DataType) -> Result<(Data, usize)> {
    match data_type {
        DataType::Int32 => {
            if offset + 4 > bytes.len() {
                bail!("Not enough bytes to decode Int32");
            }
            let value = i32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            Ok((Data::Int32(value), 4))
        }
        DataType::Int64 => {
            if offset + 8 > bytes.len() {
                bail!("Not enough bytes to decode Int64");
            }
            let value = i64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            Ok((Data::Int64(value), 8))
        }
        DataType::Float32 => {
            if offset + 4 > bytes.len() {
                bail!("Not enough bytes to decode Float32");
            }
            let value = f32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            Ok((Data::Float32(value), 4))
        }
        DataType::Float64 => {
            if offset + 8 > bytes.len() {
                bail!("Not enough bytes to decode Float64");
            }
            let value = f64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            Ok((Data::Float64(value), 8))
        }
        DataType::String => {
            let mut end = offset;
            while end < bytes.len() && bytes[end] != 0 {
                end += 1;
            }
            if end >= bytes.len() {
                bail!("String missing null terminator");
            }
            let string_bytes = &bytes[offset..end];
            let value =
                String::from_utf8(string_bytes.to_vec()).context("Invalid UTF-8 in string")?;
            Ok((Data::String(value), end - offset + 1))
        }
    }
}

pub fn encode_row(row: &Row, schema: &[ColumnCatalog]) -> Result<Vec<u8>> {
    if row.len() != schema.len() {
        bail!(
            "Row has {} values but schema expects {}",
            row.len(),
            schema.len()
        );
    }

    let mut encoded = Vec::new();

    for (value, column) in row.values.iter().zip(schema.iter()) {
        encode_value(value, &column.data_type, &mut encoded)
            .context(format!("Failed to encode column '{}'", column.name))?;
    }

    Ok(encoded)
}

fn encode_value(value: &Data, data_type: &DataType, buf: &mut Vec<u8>) -> Result<()> {
    match (value, data_type) {
        (Data::Int32(v), DataType::Int32) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (Data::Int64(v), DataType::Int64) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (Data::Float32(v), DataType::Float32) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (Data::Float64(v), DataType::Float64) => {
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (Data::String(v), DataType::String) => {
            buf.extend_from_slice(v.as_bytes());
            buf.push(0);
            Ok(())
        }
        _ => bail!(
            "Type mismatch: expected {:?} but got {:?}",
            data_type,
            value_data_type(value)
        ),
    }
}

pub fn value_data_type(value: &Data) -> DataType {
    match value {
        Data::Int32(_) => DataType::Int32,
        Data::Int64(_) => DataType::Int64,
        Data::Float32(_) => DataType::Float32,
        Data::Float64(_) => DataType::Float64,
        Data::String(_) => DataType::String,
    }
}

pub fn format_row_for_output(row: &Row) -> String {
    let formatted_values: Vec<String> = row
        .values
        .iter()
        .map(|value| match value {
            Data::Int32(v) => v.to_string(),
            Data::Int64(v) => v.to_string(),
            Data::Float32(v) => format_float32(*v),
            Data::Float64(v) => format_float64(*v),
            Data::String(v) => v.clone(),
        })
        .collect();

    formatted_values.join("|") + "|"
}

fn format_float32(f: f32) -> String {
    // SQLite format: use default float formatting with trailing zeros for decimals
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else if f == 0.0 && f.is_sign_negative() {
        "-0.0".to_string()
    } else if f.fract() == 0.0 && f.abs() < 1e6 {
        format!("{:.1}", f)
    } else {
        format!("{}", f)
    }
}

fn format_float64(f: f64) -> String {
    // SQLite format: use default float formatting with trailing zeros for decimals
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else if f == 0.0 && f.is_sign_negative() {
        "-0.0".to_string()
    } else if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.1}", f)
    } else {
        format!("{}", f)
    }
}

pub fn data_compare(left: &Data, right: &Data, operator: &common::query::ComparisionOperator) -> Result<bool> {
    use common::query::ComparisionOperator::{EQ, NE, LT, LTE, GT, GTE};
    use std::cmp::Ordering;

    let ordering = match (left, right) {
        (Data::Int32(l), Data::Int32(r)) => Some(l.cmp(r)),
        (Data::Int64(l), Data::Int64(r)) => Some(l.cmp(r)),
        (Data::Float32(l), Data::Float32(r)) => l.partial_cmp(r),
        (Data::Float64(l), Data::Float64(r)) => l.partial_cmp(r),
        (Data::String(l), Data::String(r)) => Some(l.cmp(r)),
        _ => {
            bail!(
                "Cannot compare {:?} with {:?}: type mismatch",
                value_data_type(left),
                value_data_type(right)
            );
        }
    };

    let result = match operator {
        EQ => left == right,
        NE => left != right,
        LT => ordering == Some(Ordering::Less),
        LTE => ordering == Some(Ordering::Less) || ordering == Some(Ordering::Equal),
        GT => ordering == Some(Ordering::Greater),
        GTE => ordering == Some(Ordering::Greater) || ordering == Some(Ordering::Equal),
    };

    Ok(result)
}
