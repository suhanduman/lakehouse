import subprocess, tempfile, os, yaml
from app.services.git_writer import GitWriter


def _git(cwd, *a):
    subprocess.run(["git", *a], cwd=cwd, check=True)


def _bare_repo(tmp):
    bare = os.path.join(tmp, "remote.git")
    subprocess.run(["git", "init", "--bare", "-b", "main", bare], check=True)
    seed = os.path.join(tmp, "seed")
    subprocess.run(["git", "clone", bare, seed], check=True)
    open(os.path.join(seed, "README"), "w").write("x")
    _git(seed, "add", "."); _git(seed, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "init")
    _git(seed, "push", "origin", "main")
    return bare


def _clone(bare, tmp):
    co = os.path.join(tmp, "verify"); subprocess.run(["git", "clone", bare, co], check=True)
    return co


def test_write_source_commits_files_under_path_source():
    with tempfile.TemporaryDirectory() as tmp:
        bare = _bare_repo(tmp)
        w = GitWriter(repo_url=bare, branch="main", path="pipelines", credential=None)
        res = w.write_source("mysrc", {"mysrc/00-kafkatopic-t.yaml": {"kind": "KafkaTopic", "metadata": {"name": "t"}}})
        assert res.committed
        co = _clone(bare, tmp)
        p = os.path.join(co, "pipelines", "mysrc", "00-kafkatopic-t.yaml")
        assert os.path.exists(p)
        assert yaml.safe_load(open(p))["kind"] == "KafkaTopic"


def test_remove_source_deletes_dir():
    with tempfile.TemporaryDirectory() as tmp:
        bare = _bare_repo(tmp)
        w = GitWriter(repo_url=bare, branch="main", path="pipelines", credential=None)
        w.write_source("mysrc", {"mysrc/00-x-t.yaml": {"kind": "X", "metadata": {"name": "t"}}})
        res = w.remove_source("mysrc")
        assert res.committed
        co = _clone(bare, tmp)
        assert not os.path.exists(os.path.join(co, "pipelines", "mysrc"))
