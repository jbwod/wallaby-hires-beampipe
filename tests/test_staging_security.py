import io
import json
import tarfile
from email.message import Message
from urllib.error import HTTPError

import pytest

from wallaby_hires import funcs
from wallaby_hires.funcs import (
    ManifestDownloadError,
    download_data_eval,
    download_data_ms,
    download_file,
    untar_file,
)


def _write_tar(path, members):
    with tarfile.open(path, "w") as archive:
        for name, payload, kind in members:
            info = tarfile.TarInfo(name)
            if kind == "file":
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            elif kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = payload.decode()
                archive.addfile(info)


def test_safe_tar_extraction_streams_regular_files(tmp_path):
    archive = tmp_path / "beam.tar"
    _write_tar(archive, [("beam.ms/table.dat", b"measurement-set", "file")])

    untar_file(str(archive), str(tmp_path / "out"))

    assert (tmp_path / "out/beam.ms/table.dat").read_bytes() == b"measurement-set"


@pytest.mark.parametrize(
    "member",
    [
        ("../escape", b"payload", "file"),
        ("safe/link", b"../../escape", "symlink"),
    ],
)
def test_tar_extraction_rejects_traversal_and_links(tmp_path, member):
    archive = tmp_path / "hostile.tar"
    _write_tar(archive, [member])

    with pytest.raises(ManifestDownloadError):
        untar_file(str(archive), str(tmp_path / "out"))

    assert not (tmp_path / "escape").exists()


def test_ms_retry_uses_completed_local_dataset_before_expired_url(monkeypatch, tmp_path):
    filename = "HIPASSJ1318-21_A_beam25_split.ms.tar"
    expected = tmp_path / "HIPASSJ1318-21/34166/beam25/HIPASSJ1318-21_A_beam25_split.ms"
    expected.mkdir(parents=True)
    (expected / "table.dat").write_bytes(b"complete")
    monkeypatch.setattr(
        funcs.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: pytest.fail("remote URL was opened"),
    )
    urls = json.dumps(
        [
            {
                "url": "https://example.test/expired?X-Amz-Signature=secret",
                "name": filename,
                "source_identifier": "HIPASSJ1318-21",
                "sbid": "34166",
            }
        ]
    )

    download_data_ms("", "", "", 1, "", urls, str(tmp_path))

    assert (expected / "table.dat").read_bytes() == b"complete"


def test_eval_retry_uses_local_fits_before_expired_url(monkeypatch, tmp_path):
    expected = tmp_path / "HIPASSJ1318-21/34166/eval/x/LinmosBeamImages/pb.cube.fits"
    expected.parent.mkdir(parents=True)
    expected.write_bytes(b"fits")
    monkeypatch.setattr(
        funcs.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: pytest.fail("remote URL was opened"),
    )
    urls = json.dumps(
        [
            {
                "url": "https://example.test/expired?X-Amz-Signature=secret",
                "name": "evaluation.tar",
                "source_identifier": "HIPASSJ1318-21",
                "sbid": "34166",
            }
        ]
    )

    download_data_eval("", "", "", 1, "", urls, str(tmp_path))

    assert expected.read_bytes() == b"fits"
    assert (
        tmp_path / "HIPASSJ1318-21/34166/eval/LinmosBeamImages/pb.cube.fits"
    ).read_bytes() == b"fits"


@pytest.mark.parametrize(
    "members",
    [
        [("x/LinmosBeamImages/pb.fits", b"fits", "file")],
        [("x/LinmosBeamImages/empty.cube.fits", b"", "file")],
        [
            ("x/LinmosBeamImages/first.cube.fits", b"fits-1", "file"),
            ("x/LinmosBeamImages/second.cube.fits", b"fits-2", "file"),
        ],
    ],
)
def test_evaluation_archive_requires_one_nonempty_cube_fits(tmp_path, members):
    archive = tmp_path / "evaluation.tar"
    output = tmp_path / "out"
    _write_tar(archive, members)

    with pytest.raises(ManifestDownloadError):
        untar_file(
            str(archive),
            str(output),
            member_filter=funcs._eval_calibration_tar_wanted_member,
            expected_member_count=1,
        )

    assert not list(output.rglob("*.cube.fits"))


def test_evaluation_archive_extracts_only_the_single_cube_fits(tmp_path):
    archive = tmp_path / "evaluation.tar"
    output = tmp_path / "out"
    _write_tar(
        archive,
        [
            ("x/LinmosBeamImages/pb.cube.fits", b"cube", "file"),
            ("x/LinmosBeamImages/diagnostic.fits", b"diagnostic", "file"),
        ],
    )

    untar_file(
        str(archive),
        str(output),
        member_filter=funcs._eval_calibration_tar_wanted_member,
        expected_member_count=1,
    )

    assert (output / "x/LinmosBeamImages/pb.cube.fits").read_bytes() == b"cube"
    assert not (output / "x/LinmosBeamImages/diagnostic.fits").exists()


def test_primary_beam_resolution_rejects_ambiguous_cube_fits(tmp_path):
    beam_dir = tmp_path / "LinmosBeamImages"
    beam_dir.mkdir()
    (beam_dir / "a.cube.fits").write_bytes(b"fits-1")
    (beam_dir / "b.cube.fits").write_bytes(b"fits-2")

    with pytest.raises(funcs.ManifestValidationError, match="exactly one"):
        funcs._select_primary_beam_cube_fits_in_directory(str(beam_dir))


def test_download_error_redacts_signed_url(monkeypatch, tmp_path):
    signed_url = "https://example.test/data.tar?X-Amz-Signature=top-secret"

    def fail(*_args, **_kwargs):
        raise HTTPError(signed_url, 403, "Forbidden", Message(), None)

    monkeypatch.setattr(funcs.urllib.request, "urlopen", fail)

    with pytest.raises(ManifestDownloadError) as caught:
        download_file(signed_url, True, str(tmp_path), 1)

    assert "top-secret" not in str(caught.value)
    assert str(caught.value) == "HTTP 403 downloading https://example.test/data.tar"
    assert caught.value.__cause__ is None


def test_untrusted_response_filename_cannot_escape_output(monkeypatch, tmp_path):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def info(self):
            message = Message()
            message["Content-Disposition"] = 'attachment; filename="../escape"'
            message["Content-Length"] = "7"
            return message

        def read(self, _size):
            return b"payload"

    monkeypatch.setattr(funcs.urllib.request, "urlopen", lambda *_a, **_k: Response())

    with pytest.raises(ManifestDownloadError):
        download_file("https://example.test/safe", True, str(tmp_path / "out"), 1)

    assert not (tmp_path / "escape").exists()
