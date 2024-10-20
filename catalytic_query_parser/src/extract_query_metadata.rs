use crate::crud::{extract_columns, extract_table_name, find_operation};
use catalytic::capitalizing::table_name_to_struct_name;
use catalytic::env_property_reader::keyspace;
use catalytic::query_metadata::{
    query_columns, ColumnInQuery, ParameterizedColumnType, ParameterizedValue, QueryMetadata, Ttl,
};
use catalytic::runtime::{block_on, GLOBAL_CONNECTION};
use catalytic::scylla::_macro_internal::LegacySerializedValues;
use catalytic::table_metadata::{ColumnInTable, ColumnType};
use std::collections::HashSet;

/// Extract the query meta data from a query
pub fn extract_query_meta_data(query: impl AsRef<str>) -> QueryMetadata {
    dotenvy::dotenv().unwrap();

    // Find the correct operation for the query
    let crud = find_operation(query.as_ref());
    let table_name = extract_table_name(&query, &*crud);
    let columns = query_columns(table_name);

    assert!(
        !columns.is_empty(),
        "Table '{}' in keyspace '{}' does not exists (or does not have columns, which is useless)",
        table_name,
        keyspace()
    );

    let query = query.as_ref();
    let query = replace_select_wildcard(query, &columns);
    // Before extracting the columns, remove the parameterized ttl from the query if it exists
    let extracted_columns = extract_columns(&query, &*crud);

    if query.starts_with("insert") && extracted_columns.len() != columns.len() {
        panic!("Insert query is missing values");
    }

    let ttl = extract_ttl(&query);
    let mut parameterized_columns_types =
        create_parameterized_column_types(&columns, &extracted_columns);

    if query.ends_with(" limit ?") {
        // parameterized_columns_types does not contain the limit type, add it back
        parameterized_columns_types.push(ParameterizedColumnType {
            column_type: ColumnType::Int,
            value: ParameterizedValue::Limit,
        });
    } else if let Some(Ttl::Parameterized) = ttl {
        parameterized_columns_types.push(ParameterizedColumnType {
            column_type: ColumnType::Int,
            value: ParameterizedValue::UsingTtl,
        });
    }

    // ColumnInTable can be reused in ranges, so filter duplicates
    let unique_columns_where_clause = extracted_columns
        .iter()
        .map(|r| r.column_name.clone())
        .collect::<HashSet<_>>();

    // For this variable a hashset is also used although it will not filter any elements
    // but is used for comparing later on
    let unique_columns = columns
        .iter()
        .filter(|r| r.kind().is_part_of_pk())
        .map(|r| r.column_name.clone())
        .collect::<HashSet<_>>();
    let columns_in_where_all_pk = unique_columns_where_clause.eq(&unique_columns);

    // Maybe a range is added to the last column, or an 'in' query
    let where_pattern = " where ";
    let is_selecting_unique = if let Some(start) = query.find(where_pattern) {
        let end = query.find(" limit ").unwrap_or(query.len() - 1);
        let slice = &query[start..end];

        slice.chars().filter(|i| i == &'=').count() == unique_columns.len()
    } else {
        query.ends_with("limit 1")
    };

    let is_full_pk = columns_in_where_all_pk && is_selecting_unique;

    if query.contains("count(") && query.contains(" limit ") {
        panic!("Both using count and limit is strange")
    }

    let query_type = crud.query_type(&query, is_full_pk);
    let limited = query.contains(" limit ");

    QueryMetadata {
        query,
        extracted_columns,
        parameterized_columns_types,
        query_type,
        limited,
        struct_name: table_name_to_struct_name(table_name),
        ttl,
        table_name: table_name.to_string(),
    }
}

pub fn replace_select_wildcard(query: &str, columns: &[ColumnInTable]) -> String {
    let columns_separated = columns
        .iter()
        .map(|c| c.column_name.clone())
        .collect::<Vec<_>>()
        .join(", ");

    // Wildcard should not be used: https://github.com/scylladb/scylla-rust-driver/issues/151
    if query.starts_with("select *") {
        query.replacen('*', &columns_separated, 1)
    } else {
        query.to_string()
    }
}

/// Tests is a query is correct
/// If not, it will panic
pub fn test_query(query: impl AsRef<str>) -> QueryMetadata {
    let query = query.as_ref();
    let qmd = extract_query_meta_data(query);
    let mut values = LegacySerializedValues::new();

    for parameterized_column_type in &qmd.parameterized_columns_types {
        add_random_value(&mut values, parameterized_column_type);
    }

    // Execute the query with test values
    if let Err(e) = block_on(GLOBAL_CONNECTION.query_unpaged(query, values.clone())) {
        panic!(
            "Query failed:
            Query: {}
            Result: {:#?}
            Values: {:#?}",
            qmd.query, e, values
        );
    }

    qmd
}

fn extract_ttl(query: &str) -> Option<Ttl> {
    let ttl_regex = regex::Regex::new("using ttl (.*)").unwrap();

    if let Some(m) = ttl_regex.captures(query) {
        // Extract the ttl value
        let ttl = m.get(1).unwrap().as_str();

        if ttl == "?" {
            Some(Ttl::Parameterized)
        } else {
            Some(Ttl::Fixed(ttl.parse().unwrap()))
        }
    } else {
        None
    }
}

/// Checks if all the used columns in the query are present in the table itself
/// and after that, filter out only parameterized column values
fn create_parameterized_column_types(
    columns: &[ColumnInTable],
    columns_used_in_query: &[ColumnInQuery],
) -> Vec<ParameterizedColumnType> {
    columns_used_in_query
        .iter()
        // First check if all the columns that are used are in the table definition
        .map(|cq| {
            (
                columns
                    .iter()
                    .find(|c| c.column_name.as_str() == cq.column_name.as_str())
                    .unwrap_or_else(|| panic!("Illegal column: {}", cq.column_name)),
                cq,
            )
        })
        // Only keep the parameterized values, since random values needs to be generated for that
        .filter(|(_, cq)| cq.parameterized)
        .map(|(c, cq)| {
            let column_type = ColumnType::new(c.data_type.as_str());

            ParameterizedColumnType {
                column_type,
                value: ParameterizedValue::ExtractedColumn(cq.clone()),
            }
        })
        .collect()
}

/// Generates a random value for a given data type
fn add_random_value(
    serialized_values: &mut LegacySerializedValues,
    parameterized_column_type: &ParameterizedColumnType,
) {
    let uses_in_query = match &parameterized_column_type.value {
        ParameterizedValue::ExtractedColumn(c) => c.uses_in_value,
        ParameterizedValue::UsingTtl => false,
        ParameterizedValue::Limit => false,
    };

    macro_rules! into {
        ($val: expr) => {{
            if uses_in_query {
                // Execute it with two values
                serialized_values.add_value(&vec![$val, $val]).unwrap();
            } else {
                serialized_values.add_value(&$val).unwrap();
            }
        }};
    }

    match parameterized_column_type.column_type {
        ColumnType::TinyInt => into!(i8::MAX),
        ColumnType::SmallInt => into!(i16::MAX),
        // No max here, since that will crash if generating a test value for TTL
        ColumnType::Int => into!(1),
        ColumnType::BigInt | ColumnType::Time | ColumnType::Timestamp | ColumnType::Counter => {
            into!(i64::MAX)
        }
        ColumnType::Text | ColumnType::Ascii | ColumnType::Varchar => {
            into!("_VALUE_FOR_QUERY_VALUE_TESTING")
        }
        ColumnType::Boolean => into!(true),
        ColumnType::Float => into!(f32::MAX),
        ColumnType::Double => into!(f64::MAX),
        ColumnType::Uuid => {
            into!(uuid::Uuid::parse_str("3866a82f-f37c-446c-8838-fb6686c3acf2").unwrap())
        }
        ColumnType::Custom(_) => {
            panic!("https://github.com/scylladb/scylla-rust-driver/issues/104")
        }
    }
}

#[cfg(test)]
mod query_tests {
    use super::*;
    use catalytic::query_metadata::ParameterizedValue::ExtractedColumn;
    use catalytic::query_metadata::QueryType;
    use catalytic::runtime::{query, TEST_TABLE};

    #[test]
    fn ttl() {
        assert!(extract_ttl("insert into no_ttl(c) values (1)").is_none());
        assert_eq!(
            extract_ttl("insert into no_ttl(c) values (1) using ttl 102"),
            Some(Ttl::Fixed(102))
        );
        assert_eq!(
            extract_ttl("insert into no_ttl(c) values (1) using ttl ?"),
            Some(Ttl::Parameterized)
        );
    }

    #[test]
    fn wildcard_replacement() {
        let result = test_query("select * from test_table");

        assert_eq!(result.query, "select b, c, d, a, e from test_table");
    }

    #[test]
    fn test_in() {
        let result =
            test_query("select * from test_table where b = ? and c = 5 and d in ? limit 1");

        assert_eq!(
            result.parameterized_columns_types,
            vec![
                ParameterizedColumnType {
                    column_type: ColumnType::Int,
                    value: ExtractedColumn(ColumnInQuery {
                        column_name: "b".to_string(),
                        parameterized: true,
                        uses_in_value: false,
                        is_part_of_where_clause: true,
                    },),
                },
                ParameterizedColumnType {
                    column_type: ColumnType::Int,
                    value: ExtractedColumn(ColumnInQuery {
                        column_name: "d".to_string(),
                        parameterized: true,
                        uses_in_value: true,
                        is_part_of_where_clause: true,
                    },),
                },
            ]
        );

        // Just check if they run correctly
        test_query("select * from test_table where b = ? and c = 5 and d in ? limit ?");
        test_query("select * from test_table where b = ? and c = ? and d in (1, 2) limit 1");
    }

    #[test]
    fn test_query_metadata() {
        let query = "select c from test_table where a = ? and b = 1 limit ?";
        let qmd = extract_query_meta_data(query);

        assert_eq!(
            qmd,
            QueryMetadata {
                query: query.to_string(),
                extracted_columns: vec![
                    ColumnInQuery {
                        column_name: "c".to_string(),
                        parameterized: false,
                        uses_in_value: false,
                        is_part_of_where_clause: false,
                    },
                    ColumnInQuery {
                        column_name: "a".to_string(),
                        parameterized: true,
                        uses_in_value: false,
                        is_part_of_where_clause: true,
                    },
                    ColumnInQuery {
                        column_name: "b".to_string(),
                        parameterized: false,
                        uses_in_value: false,
                        is_part_of_where_clause: true,
                    },
                ],
                parameterized_columns_types: vec![
                    ParameterizedColumnType {
                        column_type: ColumnType::Int,
                        value: ExtractedColumn(ColumnInQuery {
                            column_name: "a".to_string(),
                            parameterized: true,
                            uses_in_value: false,
                            is_part_of_where_clause: true,
                        },),
                    },
                    ParameterizedColumnType {
                        column_type: ColumnType::Int,
                        value: ParameterizedValue::Limit,
                    },
                ],
                query_type: QueryType::SelectMultiple,
                struct_name: "TestTable".to_string(),
                table_name: "test_table".to_string(),
                limited: true,
                ttl: None,
            }
        );
    }

    macro_rules! write_panic_test {
        ($name: ident, $query: expr) => {
            #[test]
            #[should_panic]
            fn $name() {
                test_query($query);
            }
        };
    }

    write_panic_test!(
        test_count_and_limit_single_query,
        format!("select count(*) from {} limit 1", TEST_TABLE)
    );
    write_panic_test!(
        test_invalid_pk,
        format!("select * from {} where a = 1 and c = 1", TEST_TABLE)
    );
    write_panic_test!(
        test_invalid_pk_another,
        format!("select * from {} where a = ? and c = 1", TEST_TABLE)
    );
    write_panic_test!(
        test_not_allow_filtering,
        format!("select * from {} where b = 1 and c > 1", TEST_TABLE)
    );

    #[test]
    fn test_general_queries() {
        test_query(format!(
            "select * from {} where b = 1 and c = ?",
            TEST_TABLE
        ));
        test_query(format!(
            "select * from {} where b = 1 and c = 1",
            TEST_TABLE
        ));
    }

    #[test]
    fn test_uuid() {
        query(
            "create table if not exists UUIDTable(u uuid, primary key((u)))",
            [],
        );

        let result = test_query("select * from UUIDTable where u = ?");
        let column_type = &result.parameterized_columns_types[0].column_type;

        match column_type {
            ColumnType::Uuid => {
                // This is correct
            }
            _ => panic!("Expected uuid"),
        }
    }
}

#[cfg(test)]
mod subset_tests {
    use crate::extract_query_metadata::create_parameterized_column_types;
    use catalytic::query_metadata::ColumnInQuery;
    use catalytic::table_metadata::ColumnInTable;

    fn check_subset_columns() -> Vec<ColumnInTable> {
        vec![
            ColumnInTable {
                column_name: "a".to_string(),
                kind: "".to_string(),
                position: 0,
                data_type: "".to_string(),
            },
            ColumnInTable {
                column_name: "b".to_string(),
                kind: "".to_string(),
                position: 0,
                data_type: "".to_string(),
            },
        ]
    }

    fn create_columns_used_in_query(value: &str) -> Vec<ColumnInQuery> {
        vec![ColumnInQuery {
            column_name: value.to_string(),
            parameterized: true,
            uses_in_value: false,
            is_part_of_where_clause: false,
        }]
    }

    #[test]
    fn test_check_subset() {
        let c = check_subset_columns();
        let r = create_parameterized_column_types(&c, &create_columns_used_in_query("a"));

        assert_eq!(1, r.len());
    }

    #[test]
    #[should_panic]
    fn test_check_subset_fail() {
        create_parameterized_column_types(
            &check_subset_columns(),
            &create_columns_used_in_query("c"),
        );
    }
}
