"""GitOps write-path: commit rendered pipeline manifests to the configured repo
(direct commit -> ArgoCD selfHeal; NO PR). Shell `git` over a temp checkout;
credential from a mounted k8s Secret (SSH key or https token), never logged."""
from __future__ import annotations

import os, shutil, subprocess, tempfile, threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class GitCredential:
    ssh_key_path: Optional[str] = None
    known_hosts_path: Optional[str] = None
    https_token: Optional[str] = None


@dataclass
class CommitResult:
    committed: bool
    ref: str = ""
    message: str = ""
    files: List[str] = field(default_factory=list)


class GitWriter:
    def __init__(self, repo_url: str, branch: str, path: str,
                 credential: Optional[GitCredential] = None) -> None:
        self.repo_url = repo_url
        self.branch = branch
        self.path = path.strip("/")
        self.credential = credential
        self._lock = threading.Lock()   # serialize pushes (single-flight)

    def _env(self) -> Dict[str, str]:
        env = dict(os.environ)
        if self.credential and self.credential.ssh_key_path:
            kh = self.credential.known_hosts_path
            opts = f"-i {self.credential.ssh_key_path} -o IdentitiesOnly=yes"
            opts += f" -o UserKnownHostsFile={kh}" if kh else " -o StrictHostKeyChecking=accept-new"
            env["GIT_SSH_COMMAND"] = f"ssh {opts}"
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        return env

    def _run(self, cwd: str, *args: str) -> str:
        return subprocess.run(["git", *args], cwd=cwd, env=self._env(),
                              check=True, capture_output=True, text=True).stdout.strip()

    def _commit_and_push(self, work: str, message: str) -> CommitResult:
        self._run(work, "-c", "user.email=console@lakehouse", "-c", "user.name=lakehouse-console",
                  "commit", "-m", message)
        try:
            self._run(work, "push", "origin", self.branch)
        except subprocess.CalledProcessError:
            self._run(work, "pull", "--rebase", "origin", self.branch)   # one retry on non-ff
            self._run(work, "push", "origin", self.branch)
        ref = self._run(work, "rev-parse", "HEAD")
        return CommitResult(committed=True, ref=ref, message=message)

    def _with_checkout(self, mutate, message: str) -> CommitResult:
        with self._lock:
            work = tempfile.mkdtemp(prefix="gitops-")
            try:
                self._run(work, "clone", "--depth", "1", "--branch", self.branch, self.repo_url, ".")
                changed = mutate(work)
                if not self._run(work, "status", "--porcelain"):
                    return CommitResult(committed=False, message="no changes", files=changed)
                self._run(work, "add", "-A")
                res = self._commit_and_push(work, message)
                res.files = changed
                return res
            finally:
                shutil.rmtree(work, ignore_errors=True)

    def write_source(self, source: str, fileset: Dict[str, Dict[str, Any]]) -> CommitResult:
        def mutate(work: str) -> List[str]:
            srcdir = os.path.join(work, self.path, source)
            shutil.rmtree(srcdir, ignore_errors=True)   # idempotent overwrite
            written = []
            for rel, manifest in sorted(fileset.items()):
                dst = os.path.join(work, self.path, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                with open(dst, "w") as fh:
                    yaml.safe_dump(manifest, fh, sort_keys=False)
                written.append(os.path.join(self.path, rel))
            return written
        return self._with_checkout(mutate, f"console: upsert source {source}")

    def remove_source(self, source: str) -> CommitResult:
        def mutate(work: str) -> List[str]:
            srcdir = os.path.join(work, self.path, source)
            existed = os.path.isdir(srcdir)
            shutil.rmtree(srcdir, ignore_errors=True)
            return [os.path.join(self.path, source)] if existed else []
        return self._with_checkout(mutate, f"console: delete source {source}")
