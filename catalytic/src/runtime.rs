use crate::env_property_reader::{database_url, keyspace, password, username};
use once_cell::sync::Lazy;
use scylla::_macro_internal::SerializeRow;
use scylla::execution_profile::ExecutionProfileBuilder;
use scylla::frame::types::Consistency;
use scylla::query::Query;
use scylla::{FromRow, IntoTypedRows, Session, SessionBuilder};
use tokio::runtime::{self, Runtime};

pub const TEST_TABLE: &str = "test_table";
pub const ANOTHER_TEST_TABLE: &str = "another_test_table";

/// The runtime can be used to use the scylla driver in non-async context (proc macro's e.g.)
pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("failed to initialize Tokio runtime")
});

pub fn query_collect_to_vec<Entity: FromRow>(
    query: impl Into<Query>,
    values: impl SerializeRow,
) -> Vec<Entity> {
    touch_global_connection();

    block_on(async move {
        GLOBAL_CONNECTION
            .query_unpaged(query, values)
            .await
            .unwrap()
            .rows
            .unwrap()
            .into_typed::<_>()
            .map(|r| r.unwrap())
            .collect()
    })
}

pub fn query(query: impl Into<Query>, values: impl SerializeRow) {
    touch_global_connection();

    block_on(async move {
        GLOBAL_CONNECTION
            .query_unpaged(query, values)
            .await
            .unwrap()
    });
}

pub fn use_keyspace(keyspace: &str) {
    touch_global_connection();

    block_on(async {
        GLOBAL_CONNECTION
            .use_keyspace(keyspace, false)
            .await
            .unwrap();
    });
}

/// Touch the global state so it gets initialized
pub fn touch_global_connection() {
    GLOBAL_CONNECTION.get_metrics();
}

pub fn set_keyspace() {
    use_keyspace(&keyspace());
}

pub fn create_test_tables() {
    touch_global_connection();

    block_on(async {
        GLOBAL_CONNECTION.query_unpaged(format!("create table if not exists {} (a int, b int, c int, d int, e int, primary key((b, c), d, a))", TEST_TABLE), []).await.unwrap();
        GLOBAL_CONNECTION.query_unpaged(format!("create table if not exists {}  (a int, b text, c text, d int, primary key((a), b, c))", ANOTHER_TEST_TABLE), []).await.unwrap();
    })
}

pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    RUNTIME.block_on(future)
}

pub async fn create_connection() -> Session {
    dotenvy::dotenv().unwrap();

    let session = SessionBuilder::new()
        .known_node(database_url())
        .user(username(), password())
        .default_execution_profile_handle(
            ExecutionProfileBuilder::default()
                .consistency(Consistency::One)
                .build()
                .into_handle(),
        )
        .build()
        .await
        .unwrap();

    session
        .query_unpaged(format!(
            "create keyspace if not exists {} with replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }} and durable_writes = false",
            keyspace()
        ), [])
        .await
        .unwrap();

    session.use_keyspace(keyspace(), false).await.unwrap();

    session
}

pub static GLOBAL_CONNECTION: Lazy<Session> = Lazy::new(|| block_on(create_connection()));

#[cfg(test)]
mod tests {
    use crate::runtime::{query, TEST_TABLE};

    #[test]
    fn it_works() {
        query(format!("select * from {}", TEST_TABLE), []);
    }
}
