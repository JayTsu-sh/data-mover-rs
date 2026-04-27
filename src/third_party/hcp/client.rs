use std::sync::Arc;

use quick_xml::de::from_str;
use reqwest::Client;
use reqwest::cookie::Jar;

use crate::Tag;
use crate::error::{Result, StorageError};

#[derive(Clone, Debug)]
pub struct HCPRestClient {
    client: Client,
    bucket_name: String,
    host: String,
}

impl HCPRestClient {
    pub fn try_new(bucket_name: String, host: String, access_key: &str, secret_access_key: &str) -> Result<Self> {
        let cookie_jar = Jar::default();
        let base_url = format!("http://{bucket_name}.{host}");
        let parsed_url = base_url
            .parse()
            .map_err(|e| StorageError::ConfigError(format!("Invalid HCP base URL '{base_url}': {e}")))?;
        cookie_jar.add_cookie_str(&format!("hcp-ns-auth={access_key}:{secret_access_key}"), &parsed_url);

        let client = Client::builder()
            .cookie_provider(Arc::new(cookie_jar))
            .build()
            .map_err(|e| StorageError::OperationError(format!("Failed to create HCP HTTP client: {e}")))?;

        Ok(HCPRestClient {
            client,
            bucket_name,
            host,
        })
    }

    pub async fn get_tags(&self, path: &str) -> Result<Vec<Tag>> {
        let url = format!(
            "http://{}.{}/rest{}?type=custom-metadata&annotation=BPM",
            self.bucket_name, self.host, path
        );

        let request = self.client.get(&url);

        let response = request
            .send()
            .await
            .map_err(|e| StorageError::OperationError(format!("Failed to send request to HCP: {e}")))?;

        if !response.status().is_success() {
            return Err(StorageError::OperationError(format!(
                "HCP request failed with status: {}",
                response.status()
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| StorageError::OperationError(format!("Failed to read HCP response: {e}")))?;

        let tags = MetaData::parse_tags(&body);
        if tags.is_empty() {
            Err(StorageError::OperationError(
                "Failed to parse HCP XML response".to_string(),
            ))
        } else {
            Ok(tags)
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ProcessInfo {
    #[serde(rename = "bpdName")]
    bpd_name: Option<String>,
    #[serde(rename = "btNo")]
    bt_no: Option<String>,
    #[serde(rename = "createTime")]
    _create_time: Option<String>,
    #[serde(rename = "currentUser")]
    _current_user: Option<String>,
    #[serde(rename = "deptName")]
    _dept_name: Option<String>,
    #[serde(rename = "deptPath")]
    _dept_path: Option<String>,
    #[serde(rename = "employeeNumber")]
    _employee_number: Option<String>,
    #[serde(rename = "orgId")]
    _org_id: Option<String>,
    _title: Option<String>,
    #[serde(rename = "userId")]
    _user_id: Option<String>,
    #[serde(rename = "userName")]
    _user_name: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct Data {
    uploader: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct MetaData {
    #[serde(rename = "processInfo")]
    process_info: ProcessInfo,
    data: Data,
}

impl MetaData {
    pub fn parse_tags(xml: &str) -> Vec<Tag> {
        if let Ok(metadata) = from_str::<MetaData>(xml) {
            let mut tags = Vec::new();

            // if let Some(val) = &metadata.process_info.create_time {
            //     tags.push(Tag {
            //         key: "createTime".to_string(),
            //         value: val.clone(),
            //     });
            // }
            if let Some(val) = &metadata.process_info.bpd_name {
                tags.push(Tag {
                    key: "bpdName".to_string(),
                    value: val.clone(),
                });
            }
            // if let Some(val) = &metadata.process_info.current_user {
            //     tags.push(Tag {
            //         key: "currentUser".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.dept_name {
            //     tags.push(Tag {
            //         key: "deptName".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.org_id {
            //     tags.push(Tag {
            //         key: "orgId".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.title {
            //     tags.push(Tag {
            //         key: "title".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.user_id {
            //     tags.push(Tag {
            //         key: "userId".to_string(),
            //         value: val.clone(),
            //     });
            // }
            if let Some(val) = &metadata.process_info.bt_no {
                tags.push(Tag {
                    key: "btNo".to_string(),
                    value: val.clone(),
                });
            }
            // if let Some(val) = &metadata.process_info.user_name {
            //     tags.push(Tag {
            //         key: "userName".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.dept_path {
            //     tags.push(Tag {
            //         key: "deptPath".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.employee_number {
            //     tags.push(Tag {
            //         key: "employeeNumber".to_string(),
            //         value: val.clone(),
            //     });
            // }
            if let Some(val) = &metadata.data.uploader {
                tags.push(Tag {
                    key: "uploader".to_string(),
                    value: val.clone(),
                });
            }

            tags
        } else {
            Vec::new()
        }
    }
}
