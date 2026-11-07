//! Conversion between protobuf `Struct` and `serde_json` values.

use envoy_types::pb::google::protobuf::{ListValue, Struct, Value, value::Kind};
use serde_json::{Map, Number};

#[must_use]
pub fn from_struct(st: Struct) -> Map<String, serde_json::Value> {
    st.fields
        .into_iter()
        .map(|(key, value)| (key, from_value(value)))
        .collect()
}

#[must_use]
pub fn to_struct(map: Map<String, serde_json::Value>) -> Struct {
    Struct {
        fields: map
            .into_iter()
            .map(|(key, value)| (key, to_value(value)))
            .collect(),
    }
}

// helpers

/// Numbers stay `f64`, as Envoy sends them; NaN, ±infinity and a missing kind become `null`.
pub(super) fn from_value(value: Value) -> serde_json::Value {
    match value.kind {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(b),
        Some(Kind::NumberValue(n)) => {
            Number::from_f64(n).map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
        Some(Kind::StringValue(s)) => serde_json::Value::String(s),
        Some(Kind::StructValue(st)) => serde_json::Value::Object(from_struct(st)),
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.into_iter().map(from_value).collect())
        }
    }
}

/// Numbers become `f64`, so integers beyond 2^53 lose precision.
fn to_value(value: serde_json::Value) -> Value {
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(b),
        serde_json::Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or_default()),
        serde_json::Value::String(s) => Kind::StringValue(s),
        serde_json::Value::Array(values) => Kind::ListValue(ListValue {
            values: values.into_iter().map(to_value).collect(),
        }),
        serde_json::Value::Object(map) => Kind::StructValue(to_struct(map)),
    };
    Value { kind: Some(kind) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let json = serde_json::json!({
            "s": "x", "n": 200.0, "f": 1.5, "b": true, "z": null, "l": [1.0, "a"], "o": {"k": "v"}
        });
        let serde_json::Value::Object(map) = json else {
            unreachable!()
        };
        assert_eq!(from_struct(to_struct(map.clone())), map);
    }

    #[test]
    fn number_edge_cases() {
        let number = |n: f64| {
            from_value(Value {
                kind: Some(Kind::NumberValue(n)),
            })
        };
        assert_eq!(number(f64::NAN), serde_json::Value::Null);
        assert_eq!(number(f64::INFINITY), serde_json::Value::Null);
        assert_eq!(number(f64::NEG_INFINITY), serde_json::Value::Null);
        assert_eq!(from_value(Value { kind: None }), serde_json::Value::Null);
        assert_eq!(number(200.0), serde_json::json!(200.0));
        assert_eq!(number(200.0).as_u64(), None);
        assert_eq!(number(1e300), serde_json::json!(1e300));
        assert_eq!(
            to_value(serde_json::json!(u64::MAX)).kind,
            Some(Kind::NumberValue(18_446_744_073_709_551_616.0))
        );
    }
}
