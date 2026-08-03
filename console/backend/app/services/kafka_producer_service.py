"""KafkaProducerService: bounded, on-demand single-message produce (Debezium
signals). Producer twin of KafkaConsumerService — lazy kafka import, injected
factory for tests, builds->sends->flushes->closes each call; never raises."""
from __future__ import annotations
import json
from typing import Any, Callable, Dict, Optional

class KafkaProducerService:
    def __init__(self, bootstrap: str, conn_kwargs: Dict[str, Any],
                 producer_factory: Optional[Callable[..., Any]] = None) -> None:
        self.bootstrap = bootstrap
        self.conn_kwargs = conn_kwargs
        if producer_factory is None:
            from kafka import KafkaProducer  # lazy
            producer_factory = lambda **kw: KafkaProducer(**kw)
        self._factory = producer_factory

    def send(self, topic: str, key: str, value: Dict[str, Any]) -> bool:
        try:
            p = self._factory(bootstrap_servers=self.bootstrap, **self.conn_kwargs)
        except Exception:  # noqa: BLE001 -- broker unreachable
            return False
        try:
            p.send(topic, key=key.encode("utf-8"), value=json.dumps(value).encode("utf-8"))
            p.flush(timeout=5)
            return True
        except Exception:  # noqa: BLE001
            return False
        finally:
            try: p.close(timeout=5)
            except Exception: pass
