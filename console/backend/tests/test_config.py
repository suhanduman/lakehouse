from app.config import Settings


def test_deploy_mode_defaults_to_direct():
    s = Settings()
    assert s.deploy_mode == "direct"


def test_gitops_fields_env_overridable(monkeypatch):
    monkeypatch.setenv("DEPLOY_MODE", "gitops")
    monkeypatch.setenv("GITOPS_REPO_URL", "ssh://git@gitea/lakehouse/pipelines.git")
    monkeypatch.setenv("GITOPS_BRANCH", "main")
    monkeypatch.setenv("GITOPS_PATH", "pipelines")
    monkeypatch.setenv("GITOPS_CREDENTIAL_SECRET", "gitops-credential")
    s = Settings()
    assert s.deploy_mode == "gitops"
    assert s.gitops_repo_url.endswith("pipelines.git")
    assert s.gitops_branch == "main"
    assert s.gitops_path == "pipelines"
    assert s.gitops_credential_secret == "gitops-credential"


def test_debezium_signal_and_notification_topics_default():
    s = Settings()
    assert s.debezium_signal_topic == "debezium-signals"
    assert s.debezium_notification_topic == "debezium-notifications"
