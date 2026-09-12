import json
import tempfile
import unittest
from pathlib import Path

from src.services.storage import JsonRepository


class JsonRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temporary_directory.name)
        self.repository = JsonRepository(self.output_dir)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_saves_entities_in_user_media_hierarchy(self) -> None:
        user_path = self.repository.save_user({"pk": "user-1", "username": "perfil"})
        media_path = self.repository.save_media(
            "user-1",
            {"pk": "media-1", "caption_text": "texto"},
        )
        story_path, created = self.repository.save_story(
            "user-1",
            {"pk": "story-1"},
        )

        self.assertEqual(user_path, self.output_dir / "users/user-1/user.json")
        self.assertEqual(
            media_path,
            self.output_dir / "users/user-1/media/media-1/media.json",
        )
        self.assertEqual(
            story_path,
            self.output_dir / "users/user-1/stories/story-1.json",
        )
        self.assertTrue(created)
        self.assertIn("collected_at", json.loads(user_path.read_text(encoding="utf-8")))

    def test_merges_comments_by_stable_id(self) -> None:
        path, added = self.repository.save_comments(
            "user-1",
            "media-1",
            [{"pk": "comment-1", "text": "original"}],
        )
        _, second_added = self.repository.save_comments(
            "user-1",
            "media-1",
            [
                {"pk": "comment-1", "text": "atualizado"},
                {"pk": "comment-2", "text": "novo"},
            ],
        )

        comments = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(added, 1)
        self.assertEqual(second_added, 1)
        self.assertEqual([item["pk"] for item in comments], ["comment-1", "comment-2"])
        self.assertEqual(comments[0]["text"], "atualizado")

    def test_does_not_overwrite_an_existing_story(self) -> None:
        path, first_created = self.repository.save_story("user-1", {"pk": "story-1"})
        _, second_created = self.repository.save_story(
            "user-1",
            {"pk": "story-1", "caption_text": "mudou"},
        )

        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertTrue(first_created)
        self.assertFalse(second_created)
        self.assertNotIn("caption_text", payload)


if __name__ == "__main__":
    unittest.main()
