pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("schema version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u32, actual: u32 },

    #[error("failed to deserialize {type_name}: {source}")]
    Deserialize {
        type_name: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

pub trait Versioned {
    const TYPE_NAME: &'static str;
    fn schema_version(&self) -> u32;
}

pub fn deserialize_versioned<T: Versioned + serde::de::DeserializeOwned>(
    json: &str,
) -> Result<T, SchemaError> {
    let value: T = serde_json::from_str(json).map_err(|source| {
        SchemaError::Deserialize { type_name: T::TYPE_NAME, source }
    })?;

    validate_schema_version(&value)?;
    Ok(value)
}

pub fn validate_schema_version(
    value: &impl Versioned,
) -> Result<(), SchemaError> {
    let actual = value.schema_version();
    if actual != SCHEMA_VERSION {
        return Err(SchemaError::VersionMismatch {
            expected: SCHEMA_VERSION,
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    struct TestDocument {
        schema_version: u32,
    }

    impl Versioned for TestDocument {
        const TYPE_NAME: &'static str = "TestDocument";

        fn schema_version(&self) -> u32 {
            self.schema_version
        }
    }

    #[test]
    fn accepts_current_schema_version() {
        let json = format!(r#"{{"schema_version": {SCHEMA_VERSION}}}"#);

        let document = deserialize_versioned::<TestDocument>(&json).unwrap();

        assert_eq!(document.schema_version, SCHEMA_VERSION);
    }

    #[test]
    fn rejects_mismatched_schema_version() {
        let actual = SCHEMA_VERSION + 1;
        let json = format!(r#"{{"schema_version": {actual}}}"#);

        let error = deserialize_versioned::<TestDocument>(&json).unwrap_err();

        assert!(matches!(
            error,
            SchemaError::VersionMismatch {
                expected: SCHEMA_VERSION,
                actual: version,
            } if version == actual
        ));
    }
}
