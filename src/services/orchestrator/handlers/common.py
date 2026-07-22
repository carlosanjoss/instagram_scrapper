from typing import Any, Dict


def model_payload(model: Any) -> Dict[str, Any]:
    return model.dict()


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
