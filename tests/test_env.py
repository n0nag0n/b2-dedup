from core.env import load_project_env


def test_load_project_env_does_not_override(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("SI_TEST_KEY=fromfile\nSI_TEST_KEEP=file\n")
    monkeypatch.setenv("SI_TEST_KEEP", "already")
    monkeypatch.delenv("SI_TEST_KEY", raising=False)
    load_project_env(env_file)
    import os

    assert os.environ["SI_TEST_KEY"] == "fromfile"
    assert os.environ["SI_TEST_KEEP"] == "already"
