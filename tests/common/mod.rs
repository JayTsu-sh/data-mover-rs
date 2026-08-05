pub(crate) trait AssertTestValue {
    type Value;

    fn assert_value(self, context: &str) -> Self::Value;
}

impl<T, E: std::fmt::Debug> AssertTestValue for Result<T, E> {
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
