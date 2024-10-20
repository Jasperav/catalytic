/// This mod contains structs which are returned from the generated code
/// The structs are quite specific in what they can do. This means you don't have generic 'execute'
/// methods, but specific methods, like 'update', 'delete' etc
use crate::Cursor;
use futures_util::{StreamExt, TryStreamExt};
use scylla::_macro_internal::{LegacySerializedValues, SerializeRow};
use scylla::cql_to_rust::FromRowError;
use scylla::frame::value::SerializeValuesError;
use scylla::query::Query;
use scylla::transport::errors::QueryError;
use scylla::transport::iterator::TypedRowIterator;
use scylla::transport::{PagingState, PagingStateResponse};
use scylla::{CachingSession, FromRow, QueryResult};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub type ScyllaQueryResult = Result<QueryResult, QueryError>;
pub type CountType = i64;
pub type TtlType = i32;

/// The Count struct is returned when a count query is executed
#[derive(scylla::FromRow, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count {
    pub count: CountType,
}

/// This error can be thrown when a unique row is expected, but this wasn't the case
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UniqueQueryRowTransformError {
    #[error("No rows in query result")]
    NoRows,
    #[error("More than one row in query result")]
    MoreThanOneRow,
    #[error("From row error`{0}`")]
    FromRowError(FromRowError),
}

/// This error can be thrown when a row is queried, but an error occurred
#[derive(Debug, Clone)]
pub enum SingleSelectQueryErrorTransform {
    UniqueQueryRowTransformError(UniqueQueryRowTransformError),
    QueryError(QueryError),
}

/// This error can be thrown when multiple rows were queried, but an error occurred
#[derive(Debug, Clone)]
pub enum MultipleSelectQueryErrorTransform {
    FromRowError(FromRowError),
    QueryError(QueryError),
}

/// This is returned when a query successfully completed, were the query could have retrieved
/// an arbitrary amount of entities
pub struct QueryEntityVecResult<T> {
    /// The queried rows
    pub entities: Vec<T>,
    /// The rows variable will be always empty here
    /// They are moved and transformed into the entities variable
    pub query_result: QueryResult,

    pub paging_result: PagingStateResponse,
}

/// Wrapper, maybe additional fields are added later
pub struct QueryEntityVec<T> {
    pub entities: Vec<T>,
}

impl<T> Deref for QueryEntityVecResult<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Vec<T> {
        &self.entities
    }
}

impl From<FromRowError> for MultipleSelectQueryErrorTransform {
    fn from(u: FromRowError) -> Self {
        MultipleSelectQueryErrorTransform::FromRowError(u)
    }
}

impl From<QueryError> for MultipleSelectQueryErrorTransform {
    fn from(u: QueryError) -> Self {
        MultipleSelectQueryErrorTransform::QueryError(u)
    }
}

impl From<UniqueQueryRowTransformError> for SingleSelectQueryErrorTransform {
    fn from(u: UniqueQueryRowTransformError) -> Self {
        SingleSelectQueryErrorTransform::UniqueQueryRowTransformError(u)
    }
}

impl From<QueryError> for SingleSelectQueryErrorTransform {
    fn from(u: QueryError) -> Self {
        SingleSelectQueryErrorTransform::QueryError(u)
    }
}

impl From<SerializeValuesError> for SingleSelectQueryErrorTransform {
    fn from(u: SerializeValuesError) -> Self {
        SingleSelectQueryErrorTransform::QueryError(u.into())
    }
}

/// This is the result of a successfully queried unique row where the unique row is optional
pub struct QueryResultUniqueRow<T> {
    pub entity: Option<T>,
    /// The rows variable will be always empty here
    /// They are moved and transformed into the entity variable
    pub query_result: QueryResult,
}

impl<T> Deref for QueryResultUniqueRow<T> {
    type Target = Option<T>;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl<T> DerefMut for QueryResultUniqueRow<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entity
    }
}

impl<T: FromRow> QueryResultUniqueRow<T> {
    fn from_query_result(
        mut query_result: QueryResult,
    ) -> Result<QueryResultUniqueRow<T>, UniqueQueryRowTransformError> {
        let mut rows = None;

        std::mem::swap(&mut query_result.rows, &mut rows);

        let mut r = rows.unwrap_or_default();

        if r.len() <= 1 {
            let entity = if r.len() == 1 {
                let entity = r.remove(0);
                match entity.into_typed() {
                    Ok(e) => Some(e),
                    Err(parse_error) => {
                        return Err(UniqueQueryRowTransformError::FromRowError(parse_error));
                    }
                }
            } else {
                None
            };
            Ok(QueryResultUniqueRow {
                query_result,
                entity,
            })
        } else {
            Err(UniqueQueryRowTransformError::MoreThanOneRow)
        }
    }
}

/// This is the result of a successfully queried unique row where the unique row is mandatory
pub struct QueryResultUniqueRowExpect<T> {
    pub entity: T,
    /// The rows variable will be always empty here
    /// They are moved and transformed into the entity variable
    pub query_result: QueryResult,
}

impl<T> Deref for QueryResultUniqueRowExpect<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl<T> DerefMut for QueryResultUniqueRowExpect<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entity
    }
}

impl<T: FromRow> QueryResultUniqueRowExpect<T> {
    fn from_query_result(
        query_result: QueryResult,
    ) -> Result<QueryResultUniqueRowExpect<T>, UniqueQueryRowTransformError> {
        QueryResultUniqueRowExpect::from_unique_row(QueryResultUniqueRow::from_query_result(
            query_result,
        )?)
    }
    fn from_unique_row(
        q: QueryResultUniqueRow<T>,
    ) -> Result<QueryResultUniqueRowExpect<T>, UniqueQueryRowTransformError> {
        match q.entity {
            Some(e) => Ok(QueryResultUniqueRowExpect {
                query_result: q.query_result,
                entity: e,
            }),
            None => Err(UniqueQueryRowTransformError::NoRows),
        }
    }
}

pub struct Qv<R: AsRef<str> = &'static str, V: SerializeRow = LegacySerializedValues> {
    pub query: R,
    pub values: V,
}

impl<R: AsRef<str> + Clone, V: SerializeRow + Clone> Clone for Qv<R, V> {
    fn clone(&self) -> Self {
        Qv {
            query: self.query.clone(),
            values: self.values.clone(),
        }
    }
}

impl<R: AsRef<str>, V: SerializeRow> Debug for Qv<R, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Qv")
            .field("query", &self.query.as_ref())
            .finish()
    }
}

impl<R: AsRef<str>, V: SerializeRow> Qv<R, V> {
    async fn execute(&self, session: &CachingSession) -> ScyllaQueryResult {
        let as_ref = self.query.as_ref();

        tracing::debug!("Executing: {}", as_ref);

        session.execute_unpaged(as_ref, &self.values).await
    }

    async fn execute_all_in_memory<T: FromRow, N>(
        &self,
        session: &CachingSession,
        page_size: i32,
        transform: impl Fn(T) -> N + Copy,
    ) -> Result<QueryEntityVec<N>, MultipleSelectQueryErrorTransform> {
        let as_ref = self.query.as_ref();

        tracing::debug!("Executing with page size: {}: {}", page_size, as_ref);

        let mut query: Query = as_ref.into();

        query.set_page_size(page_size);

        let rows = session
            .execute_iter(query, &self.values)
            .await?
            .map(|c| {
                let row = c.map(T::from_row);
                let transformed = row.map(|r| r.map(transform));

                match transformed {
                    Ok(ok) => match ok {
                        Ok(row) => Ok(row),
                        Err(err) => Err(MultipleSelectQueryErrorTransform::FromRowError(err)),
                    },
                    Err(err) => Err(MultipleSelectQueryErrorTransform::QueryError(err)),
                }
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(QueryEntityVec { entities: rows })
    }

    async fn execute_iter<T: FromRow>(
        &self,
        session: &CachingSession,
        page_size: Option<i32>,
    ) -> Result<TypedRowIterator<T>, QueryError> {
        let as_ref = self.query.as_ref();

        tracing::debug!("Executing with page size: {:#?}: {}", page_size, as_ref);

        let mut query: Query = as_ref.into();

        if let Some(p) = page_size {
            query.set_page_size(p);
        }

        let result = session.execute_iter(query, &self.values).await?;

        Ok(result.into_typed())
    }

    async fn execute_iter_paged<T: FromRow, N>(
        &self,
        session: &CachingSession,
        page_size: Option<i32>,
        paging_state: PagingState,
        transform: impl Fn(T) -> N + Copy,
    ) -> Result<QueryEntityVecResult<N>, MultipleSelectQueryErrorTransform> {
        let as_ref = self.query.as_ref();

        tracing::debug!(
            "Executing with page size: {:#?}, paging state: {:?}: {}",
            page_size,
            paging_state,
            as_ref,
        );

        let mut query: Query = as_ref.into();

        if let Some(p) = page_size {
            query.set_page_size(p);
        }

        let mut result = session
            .execute_single_page(query, &self.values, paging_state)
            .await?;
        let rows = self.transform(&mut result.0, transform)?;

        Ok(QueryEntityVecResult {
            entities: rows,
            query_result: result.0,
            paging_result: result.1,
        })
    }

    fn transform<T: FromRow, N>(
        &self,
        query_result: &mut QueryResult,
        transform: impl Fn(T) -> N + Copy,
    ) -> Result<Vec<N>, FromRowError> {
        let mut rows = None;

        std::mem::swap(&mut query_result.rows, &mut rows);

        let rows = rows.unwrap_or_default();

        // This should never fail when using exclusively the ORM (and no columns are dropped while running a server)
        rows.into_iter()
            .map(T::from_row)
            .map(|t| t.map(transform))
            .collect()
    }
}
macro_rules! simple_qv_holder {
    ($ ident : ident , $ method : ident) => {
        #[derive(Debug)]
        pub struct $ident<R: AsRef<str> = &'static str, V: SerializeRow = LegacySerializedValues> {
            pub qv: Qv<R, V>,
        }
        impl<R: AsRef<str>, V: SerializeRow> $ident<R, V> {
            pub fn new(qv: Qv<R, V>) -> Self {
                Self { qv }
            }

            pub async fn $method(&self, session: &CachingSession) -> ScyllaQueryResult {
                self.qv.execute(session).await
            }
        }

        impl<R: AsRef<str>, V: SerializeRow> Deref for $ident<R, V> {
            type Target = Qv<R, V>;

            fn deref(&self) -> &Self::Target {
                &self.qv
            }
        }

        impl<R: AsRef<str> + Clone, V: SerializeRow + Clone> Clone for $ident<R, V> {
            fn clone(&self) -> Self {
                $ident::new(self.qv.clone())
            }
        }
    };
}
simple_qv_holder!(DeleteMultiple, delete_multiple);
simple_qv_holder!(DeleteUnique, delete_unique);
simple_qv_holder!(Insert, insert);
simple_qv_holder!(Update, update);
simple_qv_holder!(Truncate, truncate);

macro_rules! read_transform {
    ($ ident : ident) => {
        #[derive(Debug)]
        pub struct $ident<
            T: FromRow,
            R: AsRef<str> = &'static str,
            V: SerializeRow = LegacySerializedValues,
        > {
            pub qv: Qv<R, V>,
            p: PhantomData<T>,
        }

        impl<T: FromRow, R: AsRef<str>, V: SerializeRow> $ident<T, R, V> {
            pub fn new(qv: Qv<R, V>) -> $ident<T, R, V> {
                $ident { qv, p: PhantomData }
            }
        }

        impl<T: FromRow, R: AsRef<str>, V: SerializeRow> Deref for $ident<T, R, V> {
            type Target = Qv<R, V>;

            fn deref(&self) -> &Self::Target {
                &self.qv
            }
        }

        impl<T: FromRow, R: AsRef<str> + Clone, V: SerializeRow + Clone> Clone for $ident<T, R, V> {
            fn clone(&self) -> Self {
                $ident::new(self.qv.clone())
            }
        }
    };
}
read_transform!(SelectMultiple);
read_transform!(SelectUnique);
read_transform!(SelectUniqueExpect);

impl<T: FromRow, R: AsRef<str>, V: SerializeRow> SelectUnique<T, R, V> {
    pub fn expect(self) -> SelectUniqueExpect<T, R, V> {
        SelectUniqueExpect::new(self.qv)
    }

    pub async fn select(
        &self,
        session: &CachingSession,
    ) -> Result<QueryResultUniqueRow<T>, SingleSelectQueryErrorTransform> {
        let result = self.qv.execute(session).await?;
        let result = QueryResultUniqueRow::from_query_result(result)?;

        Ok(result)
    }
}

impl<T: FromRow, R: AsRef<str>, V: SerializeRow> SelectUniqueExpect<T, R, V> {
    pub async fn select(
        &self,
        session: &CachingSession,
    ) -> Result<QueryResultUniqueRowExpect<T>, SingleSelectQueryErrorTransform> {
        let result = self.qv.execute(session).await?;
        let result = QueryResultUniqueRowExpect::from_query_result(result)?;

        Ok(result)
    }
}

impl<R: AsRef<str>, V: SerializeRow> SelectUniqueExpect<Count, R, V> {
    pub async fn select_count(
        &self,
        session: &CachingSession,
    ) -> Result<QueryResultUniqueRowExpect<CountType>, SingleSelectQueryErrorTransform> {
        let count: QueryResultUniqueRowExpect<Count> = self.select(session).await?;
        Ok(QueryResultUniqueRowExpect {
            entity: count.entity.count,
            query_result: count.query_result,
        })
    }
}

impl<T: FromRow, R: AsRef<str>, V: SerializeRow> SelectMultiple<T, R, V> {
    pub async fn select(
        &self,
        session: &CachingSession,
        page_size: Option<i32>,
    ) -> Result<TypedRowIterator<T>, QueryError> {
        self.qv.execute_iter(session, page_size).await
    }

    pub async fn select_paged(
        &self,
        session: &CachingSession,
        page_size: Option<i32>,
        paging_state: PagingState,
    ) -> Result<QueryEntityVecResult<T>, MultipleSelectQueryErrorTransform> {
        self.select_paged_transform(session, page_size, paging_state, |v| v)
            .await
    }

    pub async fn select_paged_transform<N>(
        &self,
        session: &CachingSession,
        page_size: Option<i32>,
        paging_state: PagingState,
        transform: impl Fn(T) -> N + Copy,
    ) -> Result<QueryEntityVecResult<N>, MultipleSelectQueryErrorTransform> {
        self.qv
            .execute_iter_paged(session, page_size, paging_state, transform)
            .await
    }

    pub async fn select_all_in_memory(
        &self,
        session: &CachingSession,
        page_size: i32,
    ) -> Result<QueryEntityVec<T>, MultipleSelectQueryErrorTransform> {
        self.select_all_in_memory_transform(session, page_size, |v| v)
            .await
    }

    pub async fn select_all_in_memory_transform<N>(
        &self,
        session: &CachingSession,
        page_size: i32,
        transform: impl Fn(T) -> N + Copy,
    ) -> Result<QueryEntityVec<N>, MultipleSelectQueryErrorTransform> {
        self.qv
            .execute_all_in_memory(session, page_size, transform)
            .await
    }
}
