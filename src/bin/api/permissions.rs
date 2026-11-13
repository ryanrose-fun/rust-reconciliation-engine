use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
pub enum Role {
    Administrator,
    InternalWriter,
    ProviderWriter,
    Auditor,
    Investigator,
}
#[derive(Clone, Debug)]
pub struct Principal {
    pub id: String,
    pub role: Role,
}
impl Principal {
    pub fn legacy() -> Self {
        Self {
            id: "legacy-administrator".into(),
            role: Role::Administrator,
        }
    }
    pub fn writes_source(&self, source: reconcile_cli::events::EventSource) -> bool {
        use reconcile_cli::events::EventSource;
        matches!(
            (self.role, source),
            (Role::Administrator, _)
                | (Role::InternalWriter, EventSource::Internal)
                | (Role::ProviderWriter, EventSource::Provider)
        )
    }
    pub fn allows(&self, method: &axum::http::Method, route: &str) -> bool {
        use axum::http::Method;
        if *method == Method::GET || *method == Method::HEAD {
            return true;
        }
        match self.role {
            Role::Administrator => true,
            Role::InternalWriter | Role::ProviderWriter => {
                *method == Method::POST && route == "/v1/events"
            }
            Role::Investigator => {
                *method == Method::POST && route == "/v1/payments/{payment_id}/investigation"
            }
            Role::Auditor => false,
        }
    }
}
