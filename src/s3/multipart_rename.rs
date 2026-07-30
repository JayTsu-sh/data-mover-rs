use super::*;

const COPY_PART_MIN: u64 = 5 * 1024 * 1024;
const COPY_PART_MAX: u64 = 5 * 1024 * 1024 * 1024;
const MAX_COPY_PARTS: u64 = 10_000;

pub(super) fn copy_part_ranges(size: u64, part_size: u64) -> Vec<(u64, u64)> {
    if size == 0 || part_size == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::with_capacity(size.div_ceil(part_size) as usize);
    let mut start = 0;
    while start < size {
        let end = (start + part_size).min(size) - 1;
        ranges.push((start, end));
        start = end + 1;
    }
    ranges
}

impl S3Storage {
    /// Copy an object entirely inside S3 using `UploadPartCopy`.
    pub(super) async fn multipart_copy_object(
        &self,
        from_key: &str,
        to_key: &str,
        size: u64,
        requested_part_size: u64,
        tags: Option<&Vec<Tag>>,
    ) -> Result<()> {
        let part_size = requested_part_size.max(size.div_ceil(MAX_COPY_PARTS));
        if !(COPY_PART_MIN..=COPY_PART_MAX).contains(&part_size) {
            return Err(StorageError::OperationError(format!(
                "S3 multipart rename part size {part_size} is outside the supported \
                 range {COPY_PART_MIN}..={COPY_PART_MAX}"
            )));
        }
        let ranges = copy_part_ranges(size, part_size);
        if ranges.len() > MAX_COPY_PARTS as usize {
            return Err(StorageError::OperationError(format!(
                "S3 multipart rename requires {} parts, exceeding the {MAX_COPY_PARTS} part limit",
                ranges.len()
            )));
        }

        let head = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(from_key)
            .send()
            .await
            .map_err(|error| {
                StorageError::S3Error(format!(
                    "HeadObject for multipart copy source {from_key} failed: {error:?}"
                ))
            })?;

        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(to_key)
            .set_metadata(head.metadata().cloned())
            .set_content_type(head.content_type().map(str::to_string))
            .set_content_encoding(head.content_encoding().map(str::to_string))
            .set_cache_control(head.cache_control().map(str::to_string))
            .set_content_disposition(head.content_disposition().map(str::to_string))
            .set_content_language(head.content_language().map(str::to_string));
        if let Some(tags) = tags
            && !tags.is_empty()
        {
            create = create.tagging(build_tagging_str(tags));
        }
        let upload_id = create
            .send()
            .await
            .map_err(|error| {
                StorageError::S3Error(format!(
                    "CreateMultipartUpload for S3 rename failed: {error:?}"
                ))
            })?
            .upload_id
            .ok_or_else(|| {
                StorageError::S3Error(
                    "CreateMultipartUpload response did not contain an upload ID".to_string(),
                )
            })?;

        let copy_source = build_copy_source(&self.bucket_name, from_key);
        let mut completed = Vec::with_capacity(ranges.len());
        for (index, (start, end)) in ranges.into_iter().enumerate() {
            let part_number = i32::try_from(index + 1).map_err(|_| {
                StorageError::OperationError("S3 multipart part number overflow".to_string())
            })?;
            let response = self
                .client
                .upload_part_copy()
                .bucket(&self.bucket_name)
                .key(to_key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .copy_source(&copy_source)
                .copy_source_range(format!("bytes={start}-{end}"))
                .send()
                .await;

            let part = match response {
                Ok(response) => response,
                Err(error) => {
                    if let Err(abort_error) = self.abort_multipart_upload(to_key, &upload_id).await
                    {
                        error!(
                            "Abort multipart S3 rename after part failure also failed: \
                             {abort_error:?}"
                        );
                    }
                    return Err(StorageError::S3Error(format!(
                        "UploadPartCopy part {part_number} failed: {error:?}"
                    )));
                }
            };
            let Some(e_tag) = part.copy_part_result().and_then(|result| result.e_tag()) else {
                let _ = self.abort_multipart_upload(to_key, &upload_id).await;
                return Err(StorageError::S3Error(format!(
                    "UploadPartCopy part {part_number} response did not contain an ETag"
                )));
            };
            completed.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build(),
            );
        }

        if let Err(error) = self
            .complete_multipart_upload(to_key, &upload_id, &completed)
            .await
        {
            if let Err(abort_error) = self.abort_multipart_upload(to_key, &upload_id).await {
                error!(
                    "Abort multipart S3 rename after completion failure also failed: \
                     {abort_error:?}"
                );
            }
            return Err(error);
        }
        Ok(())
    }
}
