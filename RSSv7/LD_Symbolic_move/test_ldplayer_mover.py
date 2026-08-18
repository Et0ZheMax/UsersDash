import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ldplayer_mover as mover


class SelectionTests(unittest.TestCase):
    def test_numbers_and_ranges(self) -> None:
        self.assertEqual(mover.parse_selection("1, 3-5", 6), {1, 3, 4, 5})

    def test_rejects_out_of_range(self) -> None:
        with self.assertRaises(ValueError):
            mover.parse_selection("1,7", 6)


@unittest.skipUnless(mover.os.name == "nt", "Windows junction test")
class TransferTests(unittest.TestCase):
    def make_vm(self, root: Path) -> tuple[mover.VM, Path]:
        source_root = root / "source" / "vms"
        destination_root = root / "destination" / "vms"
        vm_path = source_root / "leidian8"
        vm_path.mkdir(parents=True)
        destination_root.mkdir(parents=True)
        (vm_path / "disk.vmdk").write_bytes(b"test-data" * 100)
        (vm_path / "config.ini").write_text("name=test", encoding="utf-8")
        vm = mover.VM(
            name="leidian8",
            index=8,
            source=vm_path,
            destination=destination_root / "leidian8",
            is_link=False,
            destination_exists=False,
            bytes_used=909,
            logical_bytes=909,
            files=2,
        )
        return vm, destination_root

    def test_move_and_create_junction(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            vm, destination_root = self.make_vm(Path(temporary))
            warning = mover.move_one(vm, destination_root, "junction")
            self.assertIsNone(warning)
            self.assertTrue(mover.is_reparse_point(vm.source))
            self.assertEqual((vm.source / "config.ini").read_text("utf-8"), "name=test")
            self.assertTrue((vm.destination / "disk.vmdk").exists())

    def test_scan_marks_even_vm_as_recommended_and_skips_link(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_root = root / "source"
            destination_root = root / "destination"
            (source_root / "leidian7").mkdir(parents=True)
            (source_root / "leidian8").mkdir()
            moved = destination_root / "leidian10"
            moved.mkdir(parents=True)
            mover.create_directory_link(source_root / "leidian10", moved, "junction")

            found = {vm.name: vm for vm in mover.scan_vms(source_root, destination_root)}
            self.assertFalse(found["leidian7"].recommended)
            self.assertTrue(found["leidian8"].recommended)
            self.assertTrue(found["leidian10"].is_link)
            self.assertFalse(found["leidian10"].movable)

    def test_link_failure_restores_original(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            vm, destination_root = self.make_vm(Path(temporary))
            with patch.object(mover, "create_directory_link", side_effect=OSError("boom")):
                with self.assertRaises(mover.MoveError):
                    mover.move_one(vm, destination_root, "junction")
            self.assertTrue(vm.source.is_dir())
            self.assertFalse(mover.is_reparse_point(vm.source))
            self.assertTrue((vm.source / "disk.vmdk").exists())
            self.assertFalse(vm.destination.exists())

    def test_existing_destination_is_never_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            vm, destination_root = self.make_vm(Path(temporary))
            vm.destination.mkdir()
            marker = vm.destination / "belongs-to-user.txt"
            marker.write_text("keep", encoding="utf-8")
            with self.assertRaises(mover.MoveError):
                mover.move_one(vm, destination_root, "junction")
            self.assertEqual(marker.read_text("utf-8"), "keep")
            self.assertTrue((vm.source / "disk.vmdk").exists())

    def test_recovery_does_not_delete_unowned_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_root = root / "source"
            destination_root = root / "destination"
            source = source_root / "leidian8"
            destination = destination_root / "leidian8"
            source.mkdir(parents=True)
            destination.mkdir(parents=True)
            marker = destination / "belongs-to-user.txt"
            marker.write_text("keep", encoding="utf-8")
            journal = source_root / ".ldplayer-mover-leidian8.json"
            journal.write_text(
                json.dumps(
                    {
                        "vm": "leidian8",
                        "source": str(source),
                        "destination": str(destination),
                        "staging": str(destination_root / ".leidian8.copying-abc"),
                        "backup": str(source_root / ".leidian8.original-abc"),
                        "stage": "copying",
                    }
                ),
                encoding="utf-8",
            )
            mover.recover_journals(source_root, destination_root)
            self.assertEqual(marker.read_text("utf-8"), "keep")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
