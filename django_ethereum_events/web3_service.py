from django.conf import settings

from web3 import HTTPProvider, Web3
from web3.middleware import geth_poa_middleware

from .utils import Singleton


class Web3Service(metaclass=Singleton):
    """Creates a `web3` instance based on the given Provider."""

    def __init__(self, daemon, *args, **kwargs):
        """Initializes the `web3` object.

        Args:
            rpc_provider (HTTPProvider): Valid `web3` HTTPProvider instance (optional)
        """
        rpc_provider = kwargs.pop('rpc_provider', None)
        if not rpc_provider:
            if daemon.ethereum_node_timeout:
                timeout = daemon.ethereum_node_timeout
            else:
                timeout = getattr(settings, "ETHEREUM_NODE_TIMEOUT", 10)

            if daemon.ethereum_node_uri:
                uri = daemon.ethereum_node_uri
            else:
                uri = settings.ETHEREUM_NODE_URI

            rpc_provider = HTTPProvider(
                endpoint_uri=uri,
                request_kwargs={
                    "timeout": timeout
                }
            )

        self.web3 = Web3(rpc_provider)

        # If running in a network with PoA consensus, inject the middleware
        if (daemon.ethereum_geth_poa or getattr(settings, "ETHEREUM_GETH_POA", False)):
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)

        super(Web3Service, self).__init__()
