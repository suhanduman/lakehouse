from app.services.logs_hint import build_logs_hint


def test_build_logs_hint_core_recipe():
    hint = build_logs_hint(
        connector="dbz-students",
        namespace="lakehouse",
        connect_cluster="connect",
        url_template="",
    )
    assert hint["namespace"] == "lakehouse"
    assert hint["connect_pods_selector"] == (
        "strimzi.io/cluster=connect,strimzi.io/kind=KafkaConnect"
    )
    assert "dbz-students" in hint["search_terms"]
    assert "oc logs -n lakehouse" in hint["oc_command"]
    assert "strimzi.io/cluster=connect" in hint["oc_command"]
    assert "grep 'dbz-students'" in hint["oc_command"]
    assert hint["external_link"] is None


def test_build_logs_hint_includes_failed_tasks_and_cdc_logger():
    hint = build_logs_hint(
        connector="dbz-students",
        namespace="lakehouse",
        connect_cluster="connect",
        url_template="",
        failed_task_ids=[0, 2],
        is_cdc=True,
    )
    assert "task-0" in hint["search_terms"]
    assert "task-2" in hint["search_terms"]
    assert "io.debezium" in hint["search_terms"]


def test_build_logs_hint_fills_external_link_template():
    hint = build_logs_hint(
        connector="dbz-students",
        namespace="lakehouse",
        connect_cluster="connect",
        url_template="https://logs.example/{namespace}?q={connector}",
    )
    assert hint["external_link"] == "https://logs.example/lakehouse?q=dbz-students"
