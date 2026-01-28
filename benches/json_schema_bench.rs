use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use mongodb::bson::{Document, doc};
use serde_json::json;

use mstream::encoding::json_schema;
use mstream::encoding::xson::{BsonBytes, BsonBytesWithSchema, JsonBytes, JsonBytesWithSchema};
use mstream::schema::Schema;

fn sample_doc() -> Document {
    doc! {
        "name": "Alice",
        "age": 30,
        "email": "alice@example.com",
        "address": {
            "city": "London",
            "zip": "12345",
            "country": "UK"
        },
        "tags": ["rust", "mongodb", "json"],
        "metadata": {
            "created_at": "2024-01-01",
            "updated_at": "2024-01-15"
        },
        "extra_field_1": "should be dropped",
        "extra_field_2": "should be dropped",
        "extra_field_3": "should be dropped"
    }
}

fn sample_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" },
            "age": { "type": "integer" },
            "email": { "type": "string" },
            "address": {
                "type": "object",
                "properties": {
                    "city": { "type": "string" },
                    "country": { "type": "string" }
                }
            },
            "tags": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    })
}

fn bench_apply_to_doc(c: &mut Criterion) {
    let doc = sample_doc();
    let schema = sample_schema();

    c.bench_function("json_schema::apply_to_doc", |b| {
        b.iter(|| json_schema::apply_to_doc(black_box(&doc), black_box(&schema)).unwrap())
    });
}

fn bench_apply_to_vec(c: &mut Criterion) {
    let doc = sample_doc();
    let json_bytes = serde_json::to_vec(&doc).unwrap();
    let schema = sample_schema();

    c.bench_function("json_schema::apply_to_vec", |b| {
        b.iter(|| json_schema::apply_to_vec(black_box(&json_bytes), black_box(&schema)).unwrap())
    });
}

fn bench_bson_to_json_with_json_schema(c: &mut Criterion) {
    let doc = sample_doc();
    let bson_bytes = mongodb::bson::to_vec(&doc).unwrap();
    let json_schema = sample_schema();
    let schema = Schema::Json(json_schema);

    c.bench_function("bson_to_json_with_json_schema", |b| {
        b.iter(|| {
            let with_schema =
                BsonBytesWithSchema::new(black_box(bson_bytes.clone()), black_box(&schema));
            JsonBytes::try_from(with_schema).unwrap()
        })
    });
}

fn bench_bson_to_bson_with_json_schema(c: &mut Criterion) {
    let doc = sample_doc();
    let bson_bytes = mongodb::bson::to_vec(&doc).unwrap();
    let json_schema = sample_schema();
    let schema = Schema::Json(json_schema);

    c.bench_function("bson_to_bson_with_json_schema", |b| {
        b.iter(|| {
            let with_schema =
                BsonBytesWithSchema::new(black_box(bson_bytes.clone()), black_box(&schema));
            BsonBytes::try_from(with_schema).unwrap()
        })
    });
}

fn bench_json_to_json_with_json_schema(c: &mut Criterion) {
    let doc = sample_doc();
    let json_bytes = serde_json::to_vec(&doc).unwrap();
    let json_schema = sample_schema();
    let schema = Schema::Json(json_schema);

    c.bench_function("json_to_json_with_json_schema", |b| {
        b.iter(|| {
            let with_schema =
                JsonBytesWithSchema::new(black_box(json_bytes.clone()), black_box(&schema));
            JsonBytes::try_from(with_schema).unwrap()
        })
    });
}

fn bench_json_to_bson_with_json_schema(c: &mut Criterion) {
    let doc = sample_doc();
    let json_bytes = serde_json::to_vec(&doc).unwrap();
    let json_schema = sample_schema();
    let schema = Schema::Json(json_schema);

    c.bench_function("json_to_bson_with_json_schema", |b| {
        b.iter(|| {
            let with_schema =
                JsonBytesWithSchema::new(black_box(json_bytes.clone()), black_box(&schema));
            BsonBytes::try_from(with_schema).unwrap()
        })
    });
}

fn bench_bson_to_json_undefined_schema(c: &mut Criterion) {
    let doc = sample_doc();
    let bson_bytes = mongodb::bson::to_vec(&doc).unwrap();
    let schema = Schema::Undefined;

    c.bench_function("bson_to_json_undefined_schema (baseline)", |b| {
        b.iter(|| {
            let with_schema =
                BsonBytesWithSchema::new(black_box(bson_bytes.clone()), black_box(&schema));
            JsonBytes::try_from(with_schema).unwrap()
        })
    });
}

fn bench_bson_to_bson_undefined_schema(c: &mut Criterion) {
    let doc = sample_doc();
    let bson_bytes = mongodb::bson::to_vec(&doc).unwrap();
    let schema = Schema::Undefined;

    c.bench_function("bson_to_bson_undefined_schema (baseline)", |b| {
        b.iter(|| {
            let with_schema =
                BsonBytesWithSchema::new(black_box(bson_bytes.clone()), black_box(&schema));
            BsonBytes::try_from(with_schema).unwrap()
        })
    });
}

criterion_group!(
    benches,
    bench_apply_to_doc,
    bench_apply_to_vec,
    bench_bson_to_json_with_json_schema,
    bench_bson_to_bson_with_json_schema,
    bench_json_to_json_with_json_schema,
    bench_json_to_bson_with_json_schema,
    bench_bson_to_json_undefined_schema,
    bench_bson_to_bson_undefined_schema,
);
criterion_main!(benches);
