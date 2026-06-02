from pydantic import BaseModel

def convert_model_to_json(model: BaseModel) -> str:
    """Recebe qualquer instância de Pydantic Model e transforma em JSON string."""
    try:
        return model.model_dump_json(indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Erro ao converter modelo para JSON: {e}")
        raise