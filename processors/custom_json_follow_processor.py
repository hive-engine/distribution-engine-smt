# This Python file uses the following encoding: utf-8
import logging

from processors.custom_json_processor import CustomJsonProcessor, extract_user

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class FollowProcessor(CustomJsonProcessor):
    """Processor for follow operations."""

    def __init__(self, db, token_metadata):
        super().__init__(db, token_metadata)

    def process(self, ops, json_data):
        """Main process method."""
        user = extract_user(ops, json_data)
        if user is None:
            return

        if (
            isinstance(json_data, list)
            and len(json_data) == 2
            and json_data[0] == "follow"
            and isinstance(json_data[1], dict)
        ):
            if (
                "following" in json_data[1]
                and "follower" in json_data[1]
                and user == json_data[1]["follower"]
            ):
                following = f"{json_data[1]['following']}"
                muted = json_data[1]["what"] == ["ignore"]
                blog_follow = json_data[1]["what"] == ["blog"]
                follow_state = 2 if muted else 1 if blog_follow else 0
                if len(user) > 20 or len(following) > 20:
                    return
                self.followsDb.upsert(
                    {"follower": user, "following": following, "state": follow_state}
                )
