use testcontainers::{clients, core::WaitFor, images, Container};

pub struct JanusHandle<'a> {
    pub url: String,
    _container: Container<'a, images::generic::GenericImage>,
}

pub struct LocalDeps {
    docker: clients::Cli,
}

impl LocalDeps {
    pub fn new() -> Self {
        Self {
            docker: clients::Cli::default(),
        }
    }

    pub fn run_janus(&self) -> JanusHandle {
        let base_dir = env!("CARGO_MANIFEST_DIR");
        let image = images::generic::GenericImage::new(
            "cr.yandex/crp1of6bddata8ain3q5/janus-gateway",
            "v0.8.23",
        )
        .with_volume(
            format!("{}/{}", base_dir, "src/test_helpers/janus_confs/"),
            "/opt/janus/etc/janus/",
        )
        .with_entrypoint("/opt/janus/bin/janus")
        .with_wait_for(WaitFor::message_on_stdout("HTTP webserver started"));
        let node = self.docker.run(image);
        JanusHandle {
            url: format!("http://localhost:{}/janus", node.get_host_port_ipv4(8088)),
            _container: node,
        }
    }
}
