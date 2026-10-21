//! Retry only restart `SafeMode` during deletion of an already validated lab root.
use hdfs_native::HdfsError;
use std::future::Future;

pub(super) async fn delete_with_recovery_wait<F, Fut>(mut delete: F) -> super::TestResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, HdfsError>>,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_mins(2);
    for _ in 0..2 {
        loop {
            match tokio::time::timeout_at(deadline, delete()).await {
                Ok(Ok(_) | Err(HdfsError::FileNotFound(_))) => break,
                Ok(Err(HdfsError::RPCError(class, message)))
                    if class == "org.apache.hadoop.hdfs.server.namenode.SafeModeException"
                        || (class == "org.apache.hadoop.ipc.RetriableException"
                            && message.contains(
                                "org.apache.hadoop.hdfs.server.namenode.SafeModeException:",
                            )) =>
                {
                    if tokio::time::Instant::now() >= deadline {
                        return Err("HDFS cleanup remained in SafeMode for 120 seconds".into());
                    }
                    tokio::time::sleep_until(std::cmp::min(
                        deadline,
                        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
                    ))
                    .await;
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => return Err("HDFS cleanup exceeded its 120-second deadline".into()),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn safe_mode() -> HdfsError {
        HdfsError::RPCError(
            "org.apache.hadoop.ipc.RetriableException".into(),
            "org.apache.hadoop.hdfs.server.namenode.SafeModeException: Name node is in safe mode"
                .into(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_waits_for_restart_safe_mode_then_checks_idempotence()
    -> super::super::TestResult {
        let mut calls = 0;
        delete_with_recovery_wait(|| {
            calls += 1;
            std::future::ready(if calls <= 4 {
                Err(safe_mode())
            } else {
                Ok(true)
            })
        })
        .await?;
        assert_eq!(calls, 6);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_does_not_retry_other_errors() {
        for class in [
            "org.apache.hadoop.security.AccessControlException",
            "org.apache.hadoop.ipc.RetriableException",
        ] {
            let mut calls = 0;
            let result = delete_with_recovery_wait(|| {
                calls += 1;
                std::future::ready(Err(HdfsError::RPCError(
                    class.into(),
                    "unrelated failure".into(),
                )))
            })
            .await;
            assert!(result.is_err());
            assert_eq!(calls, 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_bounds_persistent_safe_mode() {
        let start = tokio::time::Instant::now();
        let result = delete_with_recovery_wait(|| std::future::ready(Err(safe_mode()))).await;
        assert!(result.is_err());
        assert_eq!(start.elapsed(), Duration::from_mins(2));
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_bounds_a_stuck_delete_rpc() {
        let start = tokio::time::Instant::now();
        let result = delete_with_recovery_wait(std::future::pending).await;
        assert!(result.is_err());
        assert_eq!(start.elapsed(), Duration::from_mins(2));
    }

    #[tokio::test]
    async fn cleanup_accepts_an_already_deleted_root() -> super::super::TestResult {
        delete_with_recovery_wait(|| {
            std::future::ready(Err(HdfsError::FileNotFound("gone".into())))
        })
        .await
    }
}
