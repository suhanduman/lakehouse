"""build_logs_hint: assemble a concrete, copy-pasteable log-inspection
recipe for a connector from static config (no live calls, no pod-log
fetching). Works whether or not an external logging stack exists -- the
`oc`-command recipe is always present; `external_link` is produced only
when a URL template is configured.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def build_logs_hint(
    *,
    connector: str,
    namespace: str,
    connect_cluster: str,
    url_template: str,
    failed_task_ids: Iterable[int] = (),
    is_cdc: bool = False,
) -> Dict[str, Any]:
    selector = f"strimzi.io/cluster={connect_cluster},strimzi.io/kind=KafkaConnect"
    search_terms: List[str] = [connector]
    for tid in failed_task_ids:
        search_terms.append(f"task-{tid}")
    if is_cdc:
        search_terms.append("io.debezium")
    oc_command = (
        f"oc logs -n {namespace} -l {selector} --tail=500 | grep '{connector}'"
    )
    external_link: Optional[str] = None
    if url_template:
        external_link = url_template.format(
            namespace=namespace, connector=connector, connect_cluster=connect_cluster
        )
    return {
        "namespace": namespace,
        "connect_pods_selector": selector,
        "search_terms": search_terms,
        "oc_command": oc_command,
        "external_link": external_link,
    }
