import os

import pytest

import cmux_herdr as ch


def test_v1_check_private_file_rejects_shared_special_and_oversized_files(tmp_path):
    path = tmp_path / "file"
    path.write_bytes(b"12345")
    path.chmod(0o600)
    ch._v1_check_private_file(path.stat(), "bad", max_size=5)
    with pytest.raises(ValueError, match="bad"):
        ch._v1_check_private_file(path.stat(), "bad", max_size=4)
    path.chmod(0o640)
    with pytest.raises(ValueError, match="bad"):
        ch._v1_check_private_file(path.stat(), "bad")
    with pytest.raises(ValueError, match="bad"):
        ch._v1_check_private_file(tmp_path.stat(), "bad")


def test_v1_private_temporary_writes_private_file_and_cleans_up(tmp_path):
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with ch._v1_private_temporary(directory_fd, "test", b"data") as (name, info):
            temporary = tmp_path / name
            assert temporary.read_bytes() == b"data"
            assert temporary.stat().st_mode & 0o777 == 0o600
            assert (info.st_dev, info.st_ino) == (
                temporary.stat().st_dev,
                temporary.stat().st_ino,
            )
            os.link(name, "installed", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        assert not temporary.exists()
        assert (tmp_path / "installed").read_bytes() == b"data"

        with ch._v1_private_temporary(directory_fd, "test", b"x") as (name, _):
            os.replace(
                name, "installed", src_dir_fd=directory_fd, dst_dir_fd=directory_fd
            )
        assert sorted(p.name for p in tmp_path.iterdir()) == ["installed"]

        with (
            pytest.raises(RuntimeError),
            ch._v1_private_temporary(directory_fd, "test", b"x") as (name, _),
        ):
            raise RuntimeError
        assert sorted(p.name for p in tmp_path.iterdir()) == ["installed"]
    finally:
        os.close(directory_fd)
