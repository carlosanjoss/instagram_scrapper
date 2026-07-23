from datetime import datetime, timezone
from typing import Any, Dict


def model_payload(model: Any) -> Dict[str, Any]:
    return model.dict()


def first_value(payload: Dict[str, Any], *field_names: str) -> Any:
    for field_name in field_names:
        value = payload.get(field_name)
        if value is not None:
            return value
    return None


def as_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def media_observation(payload: Dict[str, Any], user_id: str, username: str | None, followers_count: int | None) -> Dict[str, Any]:
    collected_at = datetime.now(timezone.utc)
    taken_at = as_iso_datetime(first_value(payload, "taken_at", "created_at"))
    age_hours = None
    if taken_at:
        try:
            age_hours = max(0.0, (collected_at - datetime.fromisoformat(taken_at)).total_seconds() / 3600)
        except ValueError:
            pass
    media_id = first_value(payload, "pk", "id")
    return {
        "schema_version": 1, "media_id": str(media_id) if media_id is not None else None,
        "user_id": str(user_id), "username": username, "collected_at": collected_at.isoformat(),
        "taken_at": taken_at, "age_hours": round(age_hours, 4) if age_hours is not None else None,
        "followers_count": followers_count, "media_type": payload.get("media_type"),
        "product_type": payload.get("product_type"), "like_count": first_value(payload, "like_count", "likes_count"),
        "comment_count": first_value(payload, "comment_count", "comments_count"),
        "view_count": first_value(payload, "view_count", "video_view_count"),
        "play_count": first_value(payload, "play_count", "video_play_count"),
        "reach": first_value(payload, "reach", "reach_count"),
        "impressions": first_value(payload, "impressions", "impression_count"),
        "non_follower_reach": first_value(payload, "non_follower_reach", "non_followers_reach"),
        "hashtag_visible": None, "caption_text": payload.get("caption_text"),
    }


def story_id(story: Any) -> str | None:
    for attribute in ("pk", "id"):
        value = getattr(story, attribute, None)
        if value is not None:
            return str(value)
    payload = model_payload(story)
    for field_name in ("pk", "id"):
        value = payload.get(field_name)
        if value is not None:
            return str(value)
    return None
