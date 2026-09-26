use std::sync::Arc;

use quick_xml::de::from_str;
use reqwest::cookie::Jar;
use reqwest::{Client, ClientBuilder};

use crate::Tag;
use crate::error::{Result, StorageError};

/// How the HCP REST client reaches its namespace: as the namespace's S3 URL does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HcpTransport {
    /// `http://`: the `hcp-ns-auth` cookie crosses the network in clear text.
    Http,
    /// `https://`, the server certificate verified unless the S3 URL says `tls_verify=false`.
    Https { verify_certificates: bool },
}

/// The HCP REST interface of one namespace, reached like the namespace's S3 URL: the same scheme,
/// host and port (`{scheme}://{namespace}.{host}[:port]`), and certificates verified unless the
/// URL says `tls_verify=false`.
#[derive(Clone, Debug)]
pub struct HCPRestClient {
    client: Client,
    /// `{scheme}://{namespace}.{host}[:port]`.
    base_url: String,
}

impl HCPRestClient {
    /// A client for namespace `bucket_name` at `authority` (`host[:port]`); the `hcp-ns-auth`
    /// cookie carries the key pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the base URL is invalid or the HTTP client cannot be built.
    pub fn try_new(
        bucket_name: &str,
        authority: &str,
        transport: HcpTransport,
        access_key: &str,
        secret_access_key: &str,
    ) -> Result<Self> {
        let accept_invalid_certs = matches!(
            transport,
            HcpTransport::Https {
                verify_certificates: false
            }
        );
        Self::with_builder(
            bucket_name,
            authority,
            transport,
            (access_key, secret_access_key),
            Client::builder().tls_danger_accept_invalid_certs(accept_invalid_certs),
        )
    }

    fn with_builder(
        bucket_name: &str,
        authority: &str,
        transport: HcpTransport,
        (access_key, secret_access_key): (&str, &str),
        builder: ClientBuilder,
    ) -> Result<Self> {
        let scheme = match transport {
            HcpTransport::Http => "http",
            HcpTransport::Https { .. } => "https",
        };
        let base_url = format!("{scheme}://{bucket_name}.{authority}");
        let parsed_url = base_url.parse().map_err(|e| {
            StorageError::ConfigError(format!("Invalid HCP base URL '{base_url}': {e}"))
        })?;
        let cookie_jar = Jar::default();
        cookie_jar.add_cookie_str(
            &format!("hcp-ns-auth={access_key}:{secret_access_key}"),
            &parsed_url,
        );
        let client = builder
            .cookie_provider(Arc::new(cookie_jar))
            .build()
            .map_err(|e| {
                StorageError::OperationError(format!("Failed to create HCP HTTP client: {e}"))
            })?;
        Ok(HCPRestClient { client, base_url })
    }

    /// The `{scheme}://{namespace}.{host}[:port]` every request goes to.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_tags(&self, path: &str) -> Result<Vec<Tag>> {
        let url = format!(
            "{}/rest{path}?type=custom-metadata&annotation=BPM",
            self.base_url
        );

        let request = self.client.get(&url);

        let response = request.send().await.map_err(|e| {
            StorageError::OperationError(format!("Failed to send request to HCP: {e}"))
        })?;

        if !response.status().is_success() {
            return Err(StorageError::OperationError(format!(
                "HCP request failed with status: {}",
                response.status()
            )));
        }

        let body = response.text().await.map_err(|e| {
            StorageError::OperationError(format!("Failed to read HCP response: {e}"))
        })?;

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
    #[must_use]
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

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
