//! Local-filesystem adapter facade.

#[allow(dead_code)]
pub(crate) mod observation;

#[allow(dead_code)]
pub(crate) mod staged;

#[cfg(test)]
pub(crate) fn test_identity(name: &str) -> crate::model::BackendIdentity {
    crate::model::BackendIdentity::new(crate::model::BackendKind::Local, name)
        .unwrap_or_else(|error| panic!("{error}"))
}
