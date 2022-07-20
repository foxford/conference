use anyhow::{Context, Result};
use std::net::IpAddr;

#[cfg(not(feature = "local_ip"))]
pub async fn get_ip(replica_label: &str) -> Result<IpAddr> {
    use k8s_openapi::api::core::v1 as api;
    use kube::{Api, Client};

    let client = Client::try_default().await?;
    let pod: Api<api::Pod> = Api::default_namespaced(client);
    let res: api::Pod = pod.get_status(replica_label).await?;

    let ip: String = res
        .status
        .expect("Failed to get status from k8s")
        .pod_ip
        .expect("Failed to get pod ip from k8s");

    let ip = ip
        .parse::<IpAddr>()
        .context("Failed to parse ip from string")?;

    Ok(ip)
}

#[cfg(feature = "local_ip")]
pub async fn get_ip(_replica_label: &str) -> Result<IpAddr> {
    let ip = local_ip_address::local_ip().context("Failed to get local ip")?;

    Ok(ip)
}
