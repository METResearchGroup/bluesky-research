"""Tests for empty-folder cleanup."""

from __future__ import annotations

from services.compact_all_services.cleanup import delete_empty_folders


def test_delete_empty_folders_removes_nested_empty_dirs(tmp_path) -> None:
    inner = tmp_path / "a" / "b"
    inner.mkdir(parents=True)
    leaf = inner / "c"
    leaf.mkdir()
    assert leaf.is_dir()

    delete_empty_folders(str(tmp_path))

    assert not leaf.exists()
    assert not inner.exists()
    assert tmp_path.is_dir()


def test_delete_empty_folders_keeps_nonempty(tmp_path) -> None:
    d = tmp_path / "keep"
    d.mkdir()
    (d / "f.txt").write_text("x")

    delete_empty_folders(str(tmp_path))

    assert d.is_dir()
    assert (d / "f.txt").exists()
