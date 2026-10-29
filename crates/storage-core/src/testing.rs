//! Shared assertion helpers for workspace contract tests.
pub trait AssertTestValue {
    type Value;

    fn assert_value(self, context: &str) -> Self::Value;
}

pub trait AssertTestError {
    type Error;

    fn assert_error(self, context: &str) -> Self::Error;
}

impl<T, E> AssertTestError for std::result::Result<T, E> {
    type Error = E;

    fn assert_error(self, context: &str) -> E {
        match self {
            Ok(_) => panic!("{context}"),
            Err(error) => error,
        }
    }
}

impl<T, E: std::fmt::Debug> AssertTestValue for std::result::Result<T, E> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error:?}"),
        }
    }
}

impl<T> AssertTestValue for Option<T> {
    type Value = T;

    fn assert_value(self, context: &str) -> T {
        match self {
            Some(value) => value,
            None => panic!("{context}"),
        }
    }
}
