use std::{collections::VecDeque, sync::Arc};

use turso_parser::ast::{self, SortOrder};

use super::{
    parse_patterns, BackingColumn, BackingIndex, BackingSchema, BackingStoreOp, BackingTable,
    IndexMethod, IndexMethodAttachment, IndexMethodConfiguration, IndexMethodContext,
    IndexMethodCursor, IndexMethodDefinition, IndexMethodMvccSupport,
};
use crate::{
    return_if_io,
    storage::btree::{BTreeKey, CursorTrait},
    translate::collate::CollationSeq,
    types::{IOResult, IOResultOr, ImmutableRecord, KeyInfo, SeekKey, SeekOp, SeekResult},
    vdbe::Register,
    vector::{
        operations::distance_cos::vector_distance_cos,
        parse_vector,
        vector_types::{Vector, VectorType},
    },
    LimboError, Result, Value,
};

pub const NAME: &str = "experimental_dense_ivf";

#[cfg(feature = "test_helper")]
static CANDIDATE_VISITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(feature = "test_helper")]
#[doc(hidden)]
pub fn take_candidate_visits() -> u64 {
    CANDIDATE_VISITS.swap(0, std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug)]
pub struct DenseIvf;

#[derive(Debug)]
struct Attachment {
    config: IndexMethodConfiguration,
    centroids: Arc<Vec<Vector<'static>>>,
    nprobe: usize,
    patterns: Vec<ast::Select>,
}

impl IndexMethod for DenseIvf {
    fn attach(&self, config: &IndexMethodConfiguration) -> Result<Arc<dyn IndexMethodAttachment>> {
        if config.columns.len() != 2 {
            return Err(invalid("requires (vector32_column, integer_tenant_column)"));
        }
        if config.parameters.get("version").and_then(Value::as_int) != Some(1)
            || config
                .parameters
                .get("metric")
                .and_then(Value::to_text)
                .map(|s| s.trim_matches('\''))
                != Some("cosine")
        {
            return Err(invalid("requires version=1 and metric='cosine'"));
        }
        let text = config
            .parameters
            .get("centroids")
            .and_then(Value::to_text)
            .map(|s| s.trim_matches('\''))
            .ok_or_else(|| invalid("requires semicolon-separated vector32 centroids"))?;
        let dims = config
            .parameters
            .get("dimensions")
            .and_then(Value::as_int)
            .filter(|d| *d > 0)
            .ok_or_else(|| invalid("requires positive dimensions"))? as usize;
        let centroids = text
            .split(';')
            .map(|s| checked_vector(&Value::build_text(s.to_owned()), dims))
            .collect::<Result<Vec<_>>>()?;
        let nprobe = config
            .parameters
            .get("nprobe")
            .and_then(Value::as_int)
            .filter(|n| *n > 0 && *n as usize <= centroids.len())
            .ok_or_else(|| invalid("nprobe must be between 1 and centroid count"))?
            as usize;
        let q = crate::util::quote_identifier;
        let pattern = format!("SELECT vector_distance_cos({}, ?1) AS distance FROM {} WHERE {} = ?2 ORDER BY distance, rowid LIMIT ?3",
            q(&config.columns[0].name), q(&config.table_name), q(&config.columns[1].name));
        Ok(Arc::new(Attachment {
            config: config.clone(),
            centroids: Arc::new(centroids),
            nprobe,
            patterns: parse_patterns(&[&pattern])?,
        }))
    }
}

impl IndexMethodAttachment for Attachment {
    fn definition(&self) -> IndexMethodDefinition<'_> {
        IndexMethodDefinition {
            method_name: NAME,
            table_name: &self.config.table_name,
            index_name: &self.config.index_name,
            patterns: &self.patterns,
            backing_btree: false,
            results_materialized: true,
            mvcc_support: IndexMethodMvccSupport::TransactionalBackingStore,
        }
    }
    fn backing_schema(&self) -> BackingSchema {
        schema(&self.config)
    }
    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(Cursor {
            schema: self.backing_schema(),
            centroids: self.centroids.clone(),
            nprobe: self.nprobe,
            store: None,
            ddl: None,
            mutation: Mutation::Init,
            search: Search::Init,
            results: VecDeque::new(),
        }))
    }
}

fn schema(config: &IndexMethodConfiguration) -> BackingSchema {
    let table = BackingTable::new(
        format!("dense_ivf_v1_{}", config.index_name),
        vec![
            BackingColumn::new("tenant", crate::schema::Type::Integer),
            BackingColumn::new("cluster", crate::schema::Type::Integer),
            BackingColumn::new("docid", crate::schema::Type::Integer),
            BackingColumn::new("vector", crate::schema::Type::Blob),
        ],
    );
    let index = BackingIndex::on_backing_table(
        &table,
        format!("{}_key", table.table_name()),
        vec![
            "tenant".into(),
            "cluster".into(),
            "docid".into(),
            "vector".into(),
        ],
        vec![
            KeyInfo {
                collation: CollationSeq::BINARY,
                sort_order: SortOrder::Asc,
                nulls_order: None
            };
            4
        ],
    );
    BackingSchema::new(vec![table], vec![index])
}

enum Mutation {
    Init,
    Seek {
        key: ImmutableRecord,
        record: ImmutableRecord,
    },
    Advance(ImmutableRecord),
    Validate(ImmutableRecord),
    Write(ImmutableRecord),
}
enum Search {
    Init,
    Seek {
        query: Vector<'static>,
        tenant: i64,
        clusters: VecDeque<usize>,
        limit: usize,
        key: ImmutableRecord,
    },
    Read {
        query: Vector<'static>,
        tenant: i64,
        clusters: VecDeque<usize>,
        limit: usize,
    },
    Next {
        query: Vector<'static>,
        tenant: i64,
        clusters: VecDeque<usize>,
        limit: usize,
    },
}
struct Cursor {
    schema: BackingSchema,
    centroids: Arc<Vec<Vector<'static>>>,
    nprobe: usize,
    store: Option<Box<dyn CursorTrait>>,
    ddl: Option<BackingStoreOp>,
    mutation: Mutation,
    search: Search,
    results: VecDeque<(i64, f64)>,
}

impl Cursor {
    fn mutate(&mut self, values: &[Register], delete: bool) -> IOResultOr<()> {
        if matches!(self.mutation, Mutation::Init) {
            let vector = checked_vector(values[0].get_value(), self.centroids[0].dims)?;
            let tenant = integer(values[1].get_value())?;
            let rowid = integer(values[2].get_value())?;
            let cluster = ranked_clusters(&vector, &self.centroids)?[0];
            let blob = crate::vector::operations::serialize::vector_serialize(vector)?;
            let key_values = [
                Value::from_i64(tenant),
                Value::from_i64(cluster as i64),
                Value::from_i64(rowid),
            ];
            let key = ImmutableRecord::from_values(&key_values, 3)?;
            let record = ImmutableRecord::from_values(
                &[
                    key_values[0].clone(),
                    key_values[1].clone(),
                    key_values[2].clone(),
                    blob,
                ],
                4,
            )?;
            self.mutation = Mutation::Seek { key, record };
        }
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| invalid("cursor not open"))?;
        loop {
            match &self.mutation {
                Mutation::Init => return Ok(IOResult::Done(())),
                Mutation::Seek { key, .. } => {
                    let found = return_if_io!(store.seek(
                        SeekKey::IndexKey(key.as_record_ref()),
                        SeekOp::GE { eq_only: true }
                    ));
                    if delete && matches!(found, SeekResult::NotFound) {
                        return Err(LimboError::Corrupt("dense IVF posting missing".into()).into());
                    }
                    let Mutation::Seek { record, .. } =
                        std::mem::replace(&mut self.mutation, Mutation::Init)
                    else {
                        unreachable!()
                    };
                    self.mutation = if delete {
                        if matches!(found, SeekResult::TryAdvance) {
                            Mutation::Advance(record)
                        } else {
                            Mutation::Validate(record)
                        }
                    } else {
                        Mutation::Write(record)
                    };
                }
                Mutation::Advance(_) => {
                    return_if_io!(store.next());
                    let Mutation::Advance(record) =
                        std::mem::replace(&mut self.mutation, Mutation::Init)
                    else {
                        unreachable!()
                    };
                    self.mutation = Mutation::Validate(record);
                }
                Mutation::Validate(expected) => {
                    let actual = return_if_io!(store.record())
                        .ok_or_else(|| invalid("posting missing after seek"))?;
                    for column in 0..3 {
                        if actual.get_value(column)? != expected.get_value(column)? {
                            return Err(LimboError::Corrupt(
                                "dense IVF posting key mismatch".into(),
                            )
                            .into());
                        }
                    }
                    let Mutation::Validate(record) =
                        std::mem::replace(&mut self.mutation, Mutation::Init)
                    else {
                        unreachable!()
                    };
                    self.mutation = Mutation::Write(record);
                }
                Mutation::Write(key) => {
                    if delete {
                        return_if_io!(store.delete());
                    } else {
                        return_if_io!(store.insert(&BTreeKey::IndexKey(key.as_record_ref())));
                    }
                    self.mutation = Mutation::Init;
                }
            }
        }
    }
    fn reset(&mut self) {
        self.mutation = Mutation::Init;
        self.search = Search::Init;
        self.results.clear();
    }
}

impl IndexMethodCursor for Cursor {
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.ddl.is_none() {
            self.ddl = Some(context.create_backing_schema(&self.schema)?);
        }
        return_if_io!(self.ddl.as_mut().unwrap().step());
        self.ddl = None;
        Ok(IOResult::Done(()))
    }
    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        if self.ddl.is_none() {
            self.ddl = Some(context.drop_backing_schema(&self.schema)?);
        }
        return_if_io!(self.ddl.as_mut().unwrap().step());
        self.ddl = None;
        Ok(IOResult::Done(()))
    }
    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.open_write(context)
    }
    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        self.store = Some(
            context
                .backing_store(&self.schema.indexes[0])?
                .ok_or_else(|| invalid("posting store missing"))?
                .open_cursor()?,
        );
        Ok(IOResult::Done(()))
    }
    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.mutate(values, false)
    }
    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.mutate(values, true)
    }
    fn abort_statement(&mut self, _: &IndexMethodContext) {
        self.reset();
    }
    fn on_transaction_rolled_back(&mut self, _: &IndexMethodContext) {
        self.reset();
    }
    fn on_savepoint_rolled_back(&mut self, _: &IndexMethodContext) {
        self.reset();
    }
    fn close(&mut self, _: &IndexMethodContext) {
        self.store = None;
        self.ddl = None;
        self.reset();
    }
    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        if matches!(self.search, Search::Init) {
            self.results.clear();
            let query = checked_vector(values[1].get_value(), self.centroids[0].dims)?;
            let tenant = integer(values[2].get_value())?;
            let limit = integer(values[3].get_value())?;
            let limit = if limit < 0 {
                usize::MAX
            } else {
                limit as usize
            };
            if limit == 0 {
                return Ok(IOResult::Done(false));
            }
            let clusters = ranked_clusters(&query, &self.centroids)?
                .into_iter()
                .take(self.nprobe)
                .collect::<VecDeque<_>>();
            let key = prefix(tenant, clusters[0])?;
            self.search = Search::Seek {
                query,
                tenant,
                clusters,
                limit,
                key,
            };
        }
        let store = self
            .store
            .as_mut()
            .ok_or_else(|| invalid("cursor not open"))?;
        loop {
            match &mut self.search {
                Search::Init => return Ok(IOResult::Done(!self.results.is_empty())),
                Search::Seek { key, .. } => {
                    let found = return_if_io!(store.seek(
                        SeekKey::IndexKey(key.as_record_ref()),
                        SeekOp::GE { eq_only: false }
                    ));
                    let Search::Seek {
                        query,
                        tenant,
                        clusters,
                        limit,
                        ..
                    } = std::mem::replace(&mut self.search, Search::Init)
                    else {
                        unreachable!()
                    };
                    self.search = if matches!(found, SeekResult::TryAdvance) {
                        Search::Next {
                            query,
                            tenant,
                            clusters,
                            limit,
                        }
                    } else {
                        Search::Read {
                            query,
                            tenant,
                            clusters,
                            limit,
                        }
                    };
                }
                Search::Read {
                    query,
                    tenant,
                    clusters,
                    limit,
                } => {
                    let record = return_if_io!(store.record());
                    let matches = if let Some(record) = record {
                        record.get_value(0)?.as_int() == Some(*tenant)
                            && record.get_value(1)?.as_int() == Some(clusters[0] as i64)
                    } else {
                        false
                    };
                    if matches {
                        #[cfg(feature = "test_helper")]
                        CANDIDATE_VISITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let record = record.unwrap();
                        let rowid = record
                            .get_value(2)?
                            .as_int()
                            .ok_or_else(|| invalid("invalid posting rowid"))?;
                        let blob = record.get_value(3)?;
                        let vector = Vector::from_slice_owned(
                            blob.to_blob()
                                .ok_or_else(|| invalid("invalid posting vector"))?,
                        )?;
                        let distance = vector_distance_cos(&vector, query)?;
                        self.results.push_back((rowid, distance));
                        let Search::Read {
                            query,
                            tenant,
                            clusters,
                            limit,
                        } = std::mem::replace(&mut self.search, Search::Init)
                        else {
                            unreachable!()
                        };
                        self.search = Search::Next {
                            query,
                            tenant,
                            clusters,
                            limit,
                        };
                    } else {
                        clusters.pop_front();
                        if clusters.is_empty() {
                            self.results
                                .make_contiguous()
                                .sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                            self.results.truncate(*limit);
                            self.search = Search::Init;
                        } else {
                            let key = prefix(*tenant, clusters[0])?;
                            let Search::Read {
                                query,
                                tenant,
                                clusters,
                                limit,
                            } = std::mem::replace(&mut self.search, Search::Init)
                            else {
                                unreachable!()
                            };
                            self.search = Search::Seek {
                                query,
                                tenant,
                                clusters,
                                limit,
                                key,
                            };
                        }
                    }
                }
                Search::Next { .. } => {
                    return_if_io!(store.next());
                    let Search::Next {
                        query,
                        tenant,
                        clusters,
                        limit,
                    } = std::mem::replace(&mut self.search, Search::Init)
                    else {
                        unreachable!()
                    };
                    self.search = Search::Read {
                        query,
                        tenant,
                        clusters,
                        limit,
                    };
                }
            }
        }
    }
    fn query_next(&mut self) -> IOResultOr<bool> {
        self.results.pop_front();
        Ok(IOResult::Done(!self.results.is_empty()))
    }
    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        Ok(IOResult::Done(self.results.front().map(|r| r.0)))
    }
    fn query_column(&mut self, _: usize) -> IOResultOr<Value> {
        Ok(IOResult::Done(Value::from_f64(
            self.results
                .front()
                .ok_or_else(|| invalid("no current result"))?
                .1,
        )))
    }
}

fn prefix(tenant: i64, cluster: usize) -> Result<ImmutableRecord> {
    ImmutableRecord::from_values(
        &[Value::from_i64(tenant), Value::from_i64(cluster as i64)],
        2,
    )
}
fn integer(value: &Value) -> Result<i64> {
    value
        .as_int()
        .ok_or_else(|| invalid("tenant and limit must be integers"))
}
fn invalid(message: &str) -> LimboError {
    LimboError::ConversionError(format!("experimental_dense_ivf: {message}"))
}
fn checked_vector(value: &Value, dims: usize) -> Result<Vector<'static>> {
    let vector = if let Some(blob) = value.to_blob() {
        Vector::from_slice_owned(blob)?
    } else {
        parse_vector(value, None)?
    };
    if vector.vector_type != VectorType::Float32Dense || vector.dims != dims {
        return Err(invalid("expected vector32 with configured dimensions"));
    }
    let values = vector.as_f32_slice();
    if values.iter().any(|v| !v.is_finite()) || values.iter().all(|v| *v == 0.0) {
        return Err(invalid("vector must be finite and nonzero"));
    }
    Ok(Vector::from_f32(values.to_vec()))
}
fn ranked_clusters(query: &Vector<'_>, centroids: &[Vector<'_>]) -> Result<Vec<usize>> {
    let mut scores = centroids
        .iter()
        .enumerate()
        .map(|(i, c)| Ok((i, vector_distance_cos(query, c)?)))
        .collect::<Result<Vec<_>>>()?;
    scores.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    Ok(scores.into_iter().map(|s| s.0).collect())
}
