use crate::table_metadata::{ColumnInTable, ColumnKind};

// The value that will be added to ColumnInTable.position if kind == clustering
const CLUSTER_KEY_PROPERTY_POSITION: i32 = 1_000;
// The value that will be given to ColumnInTable.position if kind == regular
const NON_PK_PROPERTY_POSITION: i32 = 1_000_000;

#[allow(clippy::doc_lazy_continuation)]
/// Sorts all the columns
/// It changes the position in a way that:
/// - 'partition_key' columns are processed first
/// - 'clustering' second
/// - 'regular' last
/// Within above ordering/kind, it is sorting by position, from small to large
/// clippy allow: not sure how to fix this at the caller side
#[allow(clippy::ptr_arg)]
pub(crate) fn sort_columns(c: &mut Vec<ColumnInTable>) {
    // Non primary key columns have the value '-1' for the position.
    // When generating Rust structs, non primary key columns will be the last properties.
    // Because a sorting will be done later in this method, change -1 to something big
    // so the non primary key columns will be added beneath primary key properties in Rust structs.
    for column in c.iter_mut() {
        if column.position == -1 {
            assert_eq!(ColumnKind::Regular, column.kind());
            column.position = NON_PK_PROPERTY_POSITION;
        } else if column.kind() == ColumnKind::Clustering {
            assert_ne!(-1, column.position);
            column.position += CLUSTER_KEY_PROPERTY_POSITION;
        } else {
            assert_ne!(-1, column.position);
            assert_eq!(ColumnKind::PartitionKey, column.kind())
        }
    }

    c.sort_by(|a, b| a.position.cmp(&b.position));
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sort_columns() {
        let mut columns = vec![
            ColumnInTable {
                column_name: "".to_string(),
                kind: ColumnKind::PartitionKey.to_string(),
                position: 0,
                data_type: "".to_string(),
            },
            ColumnInTable {
                column_name: "".to_string(),
                kind: ColumnKind::Clustering.to_string(),
                position: 2,
                data_type: "".to_string(),
            },
            ColumnInTable {
                column_name: "".to_string(),
                kind: ColumnKind::Regular.to_string(),
                position: -1,
                data_type: "".to_string(),
            },
            ColumnInTable {
                column_name: "".to_string(),
                kind: ColumnKind::Clustering.to_string(),
                position: 1,
                data_type: "".to_string(),
            },
            ColumnInTable {
                column_name: "".to_string(),
                kind: ColumnKind::PartitionKey.to_string(),
                position: 1,
                data_type: "".to_string(),
            },
            ColumnInTable {
                column_name: "".to_string(),
                kind: ColumnKind::Clustering.to_string(),
                position: 0,
                data_type: "".to_string(),
            },
        ];

        sort_columns(&mut columns);

        assert_eq!(0, columns[0].position);
        assert_eq!(1, columns[1].position);
        assert_eq!(CLUSTER_KEY_PROPERTY_POSITION, columns[2].position);
        assert_eq!(CLUSTER_KEY_PROPERTY_POSITION + 1, columns[3].position);
        assert_eq!(CLUSTER_KEY_PROPERTY_POSITION + 2, columns[4].position);

        assert_eq!(NON_PK_PROPERTY_POSITION, columns[5].position);
    }
}
