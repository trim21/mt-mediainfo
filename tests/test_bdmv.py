"""Tests for BDMV path computation logic in app/torrent.py."""

from app.torrent import File, _bdmv_parent, _group_by_bdmv_dir, bdmv_disc_path

# --- _bdmv_parent tests ---


def test_bdmv_parent_root_level() -> None:
    """BDMV marker directly in root: parent is the torrent root (empty tuple)."""
    assert _bdmv_parent(("BDMV", "index.bdmv")) == ()
    assert _bdmv_parent(("BDMV", "MovieObject.bdmv")) == ()


def test_bdmv_parent_backup_in_root() -> None:
    """BACKUP marker in root-level BDMV: parent is still root (empty tuple)."""
    assert _bdmv_parent(("BDMV", "BACKUP", "index.bdmv")) == ()
    assert _bdmv_parent(("BDMV", "BACKUP", "MovieObject.bdmv")) == ()


def test_bdmv_parent_multi_disc() -> None:
    """Multi-disc torrent: parent includes the disc subdirectory."""
    assert _bdmv_parent(("Disc1", "BDMV", "index.bdmv")) == ("Disc1",)
    assert _bdmv_parent(("Disc2", "BDMV", "MovieObject.bdmv")) == ("Disc2",)


def test_bdmv_parent_backup_in_multi_disc() -> None:
    """BACKUP marker in a multi-disc BDMV: disc parent, not BDMV itself."""
    assert _bdmv_parent(("Disc1", "BDMV", "BACKUP", "index.bdmv")) == ("Disc1",)
    assert _bdmv_parent(("Disc2", "BDMV", "BACKUP", "MovieObject.bdmv")) == ("Disc2",)


def test_bdmv_parent_no_bdmv_component_fallback() -> None:
    """Path without a BDMV component falls back to path[:-2]."""
    assert _bdmv_parent(("SomeDir", "index.bdmv")) == ()
    assert _bdmv_parent(("A", "B", "C", "index.bdmv")) == ("A", "B")


# --- _group_by_bdmv_dir tests ---


def test_group_single_disc_root() -> None:
    """Single BDMV disc at torrent root: one empty-parent group."""
    files = [
        File(length=100, path=("BDMV", "index.bdmv")),
        File(length=100, path=("BDMV", "MovieObject.bdmv")),
        File(length=5000, path=("BDMV", "STREAM", "00000.m2ts")),
    ]
    groups = _group_by_bdmv_dir(files)
    assert () in groups
    assert len(groups) == 1
    assert len(groups[()]) == 3  # all files belong to the root disc


def test_group_single_disc_with_backup() -> None:
    """BDMV disc with BACKUP markers: should still produce one empty-parent group."""
    files = [
        File(length=100, path=("BDMV", "index.bdmv")),
        File(length=100, path=("BDMV", "MovieObject.bdmv")),
        File(length=100, path=("BDMV", "BACKUP", "index.bdmv")),
        File(length=100, path=("BDMV", "BACKUP", "MovieObject.bdmv")),
        File(length=5000, path=("BDMV", "STREAM", "00000.m2ts")),
    ]
    groups = _group_by_bdmv_dir(files)
    assert () in groups
    assert len(groups) == 1
    assert len(groups[()]) == 5


def test_group_multi_disc() -> None:
    """Two BDMV discs: each should form its own group."""
    files = [
        # Disc1 files
        File(length=100, path=("Disc1", "BDMV", "index.bdmv")),
        File(length=100, path=("Disc1", "BDMV", "MovieObject.bdmv")),
        File(length=3000, path=("Disc1", "BDMV", "STREAM", "00000.m2ts")),
        File(length=100, path=("Disc1", "BDMV", "BACKUP", "index.bdmv")),
        # Disc2 files (larger stream)
        File(length=100, path=("Disc2", "BDMV", "index.bdmv")),
        File(length=100, path=("Disc2", "BDMV", "MovieObject.bdmv")),
        File(length=5000, path=("Disc2", "BDMV", "STREAM", "00000.m2ts")),
        File(length=100, path=("Disc2", "BDMV", "BACKUP", "MovieObject.bdmv")),
    ]
    groups = _group_by_bdmv_dir(files)
    assert ("Disc1",) in groups
    assert ("Disc2",) in groups
    assert len(groups) == 2
    assert len(groups[("Disc1",)]) == 4
    assert len(groups[("Disc2",)]) == 4


def test_group_no_bdmv_markers() -> None:
    """Files without any BDMV markers: returns empty dict."""
    files = [
        File(length=100, path=("video.mkv",)),
        File(length=200, path=("subs", "subtitle.srt")),
    ]
    groups = _group_by_bdmv_dir(files)
    assert groups == {}


# --- bdmv_disc_path tests ---


def test_disc_path_root_level() -> None:
    """BDMV at torrent root: disc path is the save_path itself."""
    files = [
        File(length=100, path=("BDMV", "index.bdmv")),
        File(length=5000, path=("BDMV", "STREAM", "00000.m2ts")),
    ]
    assert bdmv_disc_path(files, "/downloads/movie") == "/downloads/movie"


def test_disc_path_multi_disc() -> None:
    """Multi-disc: disc path appends the disc subdirectory."""
    files = [
        File(length=100, path=("Disc1", "BDMV", "index.bdmv")),
        File(length=3000, path=("Disc1", "BDMV", "STREAM", "00000.m2ts")),
        File(length=100, path=("Disc2", "BDMV", "index.bdmv")),
        File(length=5000, path=("Disc2", "BDMV", "STREAM", "00000.m2ts")),
    ]
    assert bdmv_disc_path(files, "/downloads/movie") == "/downloads/movie/Disc2"


def test_disc_path_with_backup_ignored() -> None:
    """BACKUP markers should not cause BDMV to be treated as a disc parent."""
    files = [
        File(length=100, path=("Disc2", "BDMV", "BACKUP", "index.bdmv")),
        File(length=100, path=("Disc2", "BDMV", "BACKUP", "MovieObject.bdmv")),
        File(length=100, path=("Disc2", "BDMV", "index.bdmv")),
        File(length=5000, path=("Disc2", "BDMV", "STREAM", "00000.m2ts")),
    ]
    result = bdmv_disc_path(files, "/downloads/movie")
    assert result == "/downloads/movie/Disc2"
    assert not result.endswith("/BDMV")


def test_disc_path_no_files() -> None:
    """Empty file list returns save_path."""
    assert bdmv_disc_path([], "/downloads/movie") == "/downloads/movie"
