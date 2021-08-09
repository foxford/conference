use testcontainers::{clients, core::WaitFor, images, Container, Image};

pub struct JanusHandle<'a> {
    pub url: String,
    _container: Container<'a, images::generic::GenericImage>,
}

pub struct PostgresHandle<'a> {
    pub connection_string: String,
    _container: Container<'a, images::postgres::Postgres>,
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

    pub fn run_postgres(&self) -> PostgresHandle {
        let image = images::postgres::Postgres::default();
        let node = self.docker.run(image);
        let connection_string = format!(
            "postgres://postgres:postgres@localhost:{}",
            node.get_host_port(5432),
        );
        PostgresHandle {
            connection_string,
            _container: node,
        }
    }

    pub fn run_janus(&self) -> JanusHandle {
        let base_dir = env!("CARGO_MANIFEST_DIR");
        let image = images::generic::GenericImage::new("foxford/janus-gateway:5c21ee9")
            .with_volume(
                format!("{}/{}", base_dir, "src/test_helpers/janus_confs/"),
                "/opt/janus/etc/janus/",
            )
            .with_entrypoint("/opt/janus/bin/janus")
            .with_wait_for(WaitFor::message_on_stdout("HTTP webserver started"));
        let node = self.docker.run(image);
        JanusHandle {
            url: format!("http://localhost:{}/janus", node.get_host_port(8088)),
            _container: node,
        }
    }
}
