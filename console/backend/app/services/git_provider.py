"""B-v2 GitOps — pipeline manifestlerini pipeline repo'suna bir PR olarak açan
sağlayıcı soyutlaması. SCM/CI kararı (GitLab / GitHub / Gitea) HENÜZ BELİRSİZ
olduğundan mantık SCM-agnostiktir: `gitops_render.render_gitops_pipeline`'ın
ürettiği manifest setini alır; somut PR mekaniği sağlayıcıya bırakılır.

- `LocalDirGitProvider`: manifestleri bir dizine `<source>.yaml` (çok-dokümanlı)
  olarak yazar — pipeline repo checkout'unu simüle eder. Dry-run / test /
  air-gapped B-v2 için varsayılan; gerçek PR açmaz.
- `GitlabGitProvider` / `GithubGitProvider`: SCM kararı verilince doldurulacak
  stub'lar (branch → commit → push → MR/PR). Şu an NotImplementedError.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Protocol

import yaml


def manifests_to_yaml(manifests: List[Dict[str, Any]]) -> str:
    """Manifest listesini çok-dokümanlı YAML'a çevirir (pipeline dosyası içeriği)."""
    return "---\n".join(yaml.safe_dump(m, sort_keys=False) for m in manifests)


class GitProvider(Protocol):
    def open_pipeline_pr(
        self, *, source_name: str, manifests: List[Dict[str, Any]], title: str, body: str
    ) -> Dict[str, Any]:
        """Pipeline manifest setini pipeline repo'suna PR olarak açar. Dönüş:
        en az {'url': ...} içeren bir özet (Console UI PR durumunu gösterir)."""
        ...


class LocalDirGitProvider:
    """Manifestleri `<root>/<source>.yaml` olarak yazar (pipeline repo'yu simüle
    eder). SCM kararına kadar test edilebilir / air-gapped varsayılan sağlayıcı.
    Gerçek PR AÇMAZ — yazdığı dosya yolunu döner."""

    def __init__(self, root: str) -> None:
        self.root = root

    def open_pipeline_pr(
        self, *, source_name: str, manifests: List[Dict[str, Any]], title: str, body: str
    ) -> Dict[str, Any]:
        os.makedirs(self.root, exist_ok=True)
        path = os.path.join(self.root, f"{source_name}.yaml")
        with open(path, "w") as fh:
            fh.write(manifests_to_yaml(manifests))
        return {
            "provider": "local",
            "url": f"file://{os.path.abspath(path)}",
            "path": path,
            "title": title,
            "manifests": len(manifests),
        }


class _UnconfiguredGitProvider:
    """SCM seçimi yapılana kadar gerçek Git sağlayıcıları için placeholder."""

    name = "unconfigured"

    def open_pipeline_pr(self, **_: Any) -> Dict[str, Any]:  # noqa: D401
        raise NotImplementedError(
            f"{self.name} Git sağlayıcısı henüz yapılandırılmadı — SCM/CI kararı "
            "(GitLab/GitHub/Gitea) bekliyor. Şimdilik LocalDirGitProvider kullanın "
            "veya doğrudan-apply (B-v1) orchestrator modunda kalın. "
            "Bkz. docs/superpowers/specs/2026-07-19-console-gitops-b-v2-design.md."
        )


class GitlabGitProvider(_UnconfiguredGitProvider):
    name = "gitlab"


class GithubGitProvider(_UnconfiguredGitProvider):
    name = "github"
