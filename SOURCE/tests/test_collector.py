import json
import tempfile
import unittest
from pathlib import Path

from src.services.collector import InstagramJsonCollector
from src.services.storage import JsonRepository


class FakeInstagramClient:
    def __init__(self) -> None:
        self.comments_calls = 0

    def user_info_by_username_v1(self, username):
        return {
            "pk": "user-1",
            "username": username,
            "follower_count": 100,
            "following_count": 20,
            "media_count": 1,
            "is_private": False,
        }

    def user_stories(self, user_id, amount):
        return [{"pk": "story-1", "user_id": user_id}][:amount]

    def user_medias(self, user_id, amount):
        return [{"pk": "media-1", "user_id": user_id}][:amount]

    def media_info(self, media_id):
        return {
            "pk": media_id,
            "user": {"pk": "user-1"},
            "comment_count": 1,
            "like_count": 5,
        }

    def media_comments(self, media_id, amount):
        self.comments_calls += 1
        return [{"pk": "comment-1", "media_id": media_id, "text": "comentário"}][:amount]


class InstagramJsonCollectorTests(unittest.TestCase):
    def test_collects_to_json_and_skips_unchanged_comments_next_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output_dir = Path(temporary_directory)
            client = FakeInstagramClient()
            collector = InstagramJsonCollector(
                client=client,
                repository=JsonRepository(output_dir),
                min_delay_seconds=0,
                max_delay_seconds=0,
                sleep=lambda _seconds: None,
            )

            first = collector.collect_user("perfil", media_limit=1, stories_limit=1, comments_limit=10)
            second = collector.collect_user("perfil", media_limit=1, stories_limit=1, comments_limit=10)

            comments_path = output_dir / "users/user-1/media/media-1/comments.json"
            observations_path = output_dir / "users/user-1/media/media-1/observations.json"
            self.assertEqual(first["comments_added"], 1)
            self.assertEqual(second["comments_added"], 0)
            self.assertEqual(client.comments_calls, 1)
            self.assertEqual(len(json.loads(comments_path.read_text(encoding="utf-8"))), 1)
            self.assertEqual(len(json.loads(observations_path.read_text(encoding="utf-8"))), 2)

    def test_zero_limits_do_not_request_unbounded_collections(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            client = FakeInstagramClient()
            collector = InstagramJsonCollector(
                client=client,
                repository=JsonRepository(temporary_directory),
                min_delay_seconds=0,
                max_delay_seconds=0,
                sleep=lambda _seconds: None,
            )

            result = collector.collect_user(
                "perfil",
                media_limit=0,
                stories_limit=0,
                comments_limit=0,
            )

            self.assertEqual(result["media_observed"], 0)
            self.assertEqual(result["stories_added"], 0)
            self.assertEqual(client.comments_calls, 0)


if __name__ == "__main__":
    unittest.main()
