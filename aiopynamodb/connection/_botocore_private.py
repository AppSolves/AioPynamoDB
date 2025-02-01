"""
Type-annotates the private aiobotocore APIs that we're currently relying on.
"""
from typing import Dict, Awaitable

import aiobotocore.client
import aiobotocore.credentials
import aiobotocore.endpoint
import aiobotocore.hooks
import aiobotocore.signers


class BotocoreEndpointPrivate(aiobotocore.endpoint.Endpoint):
    _event_emitter: aiobotocore.hooks.HierarchicalEmitter


class BotocoreRequestSignerPrivate(aiobotocore.signers.RequestSigner):
    _credentials: aiobotocore.credentials.Credentials


class BotocoreBaseClientPrivate(aiobotocore.client.BaseClient):
    _endpoint: BotocoreEndpointPrivate
    _request_signer: BotocoreRequestSignerPrivate

    async def _make_api_call(
        self,
        operation_name: str,
        operation_kwargs: Dict,
    ) -> Dict:
        raise NotImplementedError