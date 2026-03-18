"""Volcengine (火山云) LLM client wrapper using langchain-openai."""

from langchain_openai import ChatOpenAI
from openai import OpenAI

from shared.config import CONFIG

_vc = CONFIG["volcengine"]


def get_chat_model(temperature: float = 0.7, **kwargs) -> ChatOpenAI:
    """Return a ChatOpenAI instance pointing at the Volcengine ARK endpoint."""
    return ChatOpenAI(
        model=_vc["seed_model"],
        openai_api_key=_vc["api_key"],
        openai_api_base=_vc["base_url"],
        temperature=temperature,
        **kwargs,
    )


def get_vision_model(temperature: float = 0.2, **kwargs) -> ChatOpenAI:
    """Return a ChatOpenAI instance for the Volcengine vision model."""
    return ChatOpenAI(
        model=_vc["vision_model"],
        openai_api_key=_vc["api_key"],
        openai_api_base=_vc["base_url"],
        temperature=temperature,
        **kwargs,
    )


def get_seedance_model(temperature: float = 0.2, **kwargs) -> ChatOpenAI:
    """抖音视频特征抽取专用（seedance 模型）；未配置时回退到 seed_model。"""
    model = _vc.get("seedance_model") or _vc["seed_model"]
    return ChatOpenAI(
        model=model,
        openai_api_key=_vc["api_key"],
        openai_api_base=_vc["base_url"],
        temperature=temperature,
        **kwargs,
    )


def get_seed2_client(**kwargs) -> OpenAI:
    """Return raw OpenAI-compatible client for multimodal/audio calls."""
    return OpenAI(
        api_key=_vc["api_key"],
        base_url=_vc["base_url"],
        **kwargs,
    )
