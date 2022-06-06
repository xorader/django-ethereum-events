import itertools
import json
import logging

from django.conf import settings
from django.core.cache import cache
from django.utils.module_loading import import_string
from web3.types import FilterParams

from .decoder import Decoder
from .exceptions import UnknownBlock
from .models import CACHE_UPDATE_KEY, Daemon, FailedEventLog
from .utils import HexJsonEncoder, refresh_cache_update_value
from .web3_service import Web3Service

logger = logging.getLogger(__name__)

class EventListener:
    """Event Listener class."""

    def __init__(self, daemon=None, *args, **kwargs):
        super(EventListener, self).__init__()
        if daemon is None:
            self.daemon = Daemon.get_default_daemon()
        else:
            self.daemon = daemon
        self.decoder = Decoder(block_number=self.daemon.block_number + 1, blockchain_id=self.daemon.blockchain_id)
        self.web3 = Web3Service(self.daemon, *args, **kwargs).web3

        if self.daemon.ethereum_logs_batch_size:
            self.ethereum_logs_batch_size = self.daemon.ethereum_logs_batch_size
        else:
            self.ethereum_logs_batch_size = getattr(settings, "ETHEREUM_LOGS_BATCH_SIZE", 10000)
        if self.daemon.ethereum_logs_filter_available is not None:
            self.ethereum_logs_filter_available = self.daemon.ethereum_logs_filter_available
        else:
            self.ethereum_logs_filter_available = getattr(settings, "ETHEREUM_LOGS_FILTER_AVAILABLE", False)
        if self.daemon.ethereum_logs_filter_getlogs is not None:
            self.ethereum_logs_filter_getlogs = self.daemon.ethereum_logs_filter_getlogs
        else:
            self.ethereum_logs_filter_getlogs = getattr(settings, "ETHEREUM_LOGS_FILTER_GETLOGS", False)

    def _get_block_range(self):
        current = self.web3.eth.blockNumber
        step = self.ethereum_logs_batch_size
        if self.daemon.block_number < current:
            start = self.daemon.block_number + 1
            return start, min(current, start + step)

        return None, None

    def get_pending_blocks(self):
        """
        Retrieve the blocks that have not been processed.

        Returns:
            An iterable of (from, to) tuples, containing
            the unprocessed block numbers.

        """
        start, end = self._get_block_range()
        if start is None:
            return []
        else:
            return list(range(start, end + 1))

    def update_block_number(self, block_number):
        """Updates the internal block_number counter."""
        self.daemon.block_number = block_number
        self.daemon.save()

    def get_block_logs(self, block_number):
        """Retrieves the relevant log entries from the given block.

        Args:
            block_number (int): The block number of the block to process.
        Returns:
            The list of relevant log entries.

        """
        block = self.web3.eth.getBlock(block_number)
        relevant_logs = []
        if block and block.get('hash'):
            for tx in block['transactions']:
                receipt = self.web3.eth.getTransactionReceipt(tx)

                if receipt is None:
                    continue

                for log in receipt.get('logs', []):
                    address = log['address']
                    topic = log['topics'][0].hex()
                    if (address, topic) in self.decoder.monitored_events:
                        relevant_logs.append(log)
            return relevant_logs
        else:
            raise UnknownBlock

    def get_logs(self, from_block, to_block):
        """
        Retrieves the relevant log entries from the given block range.

        Args:
            from_block (int): The first block number.
            to_block (int): The last block number.

        Returns:
            The list of relevant log entries.

        """
        logs = itertools.chain.from_iterable(
            self.get_block_logs(n) for n in range(from_block, to_block + 1))
        return list(logs)

    def save_events(self, decoded_logs):
        """
        Fires the appropriate event receivers for every given log.

        Args:
            decoded_logs (:obj:`list` of :obj:`dict`): The decoded logs.

        """
        for (address, topic), decoded_log in decoded_logs:
            event_receiver = self.decoder.monitored_events[(address, topic)].event_receiver

            try:
                event_receiver_cls = import_string(event_receiver)
                event_receiver_cls().save(decoded_event=decoded_log, blockchain_id=self.daemon.blockchain_id)
            except Exception:
                # Save the event information that caused the exception
                failed_event = FailedEventLog.objects.create(
                    event=decoded_log.event,
                    transaction_hash=decoded_log.transactionHash.hex(),
                    transaction_index=decoded_log.transactionIndex,
                    block_hash=decoded_log.blockHash.hex(),
                    block_number=decoded_log.blockNumber,
                    log_index=decoded_log.logIndex,
                    address=decoded_log.address,
                    args=json.dumps(decoded_log.args, cls=HexJsonEncoder),
                    monitored_event=self.decoder.monitored_events[(address, topic)]
                )

                logger.error('Exception while calling {0}. FailedEventLog entry with id {1} created.'.format(
                    event_receiver, failed_event.pk), exc_info=True)

    def check_for_state_updates(self, block_number):
        """If a MonitoredEvent has been added, updated, deleted, the decoder state is updated.

        Args:
            block_number: current working block

        """
        update_required = cache.get(CACHE_UPDATE_KEY, False)
        if update_required:
            self.decoder.refresh_state(block_number=block_number)
            refresh_cache_update_value(update_required=False)

    def execute(self):
        """Program loop, does all the underlying work."""
        if self.ethereum_logs_filter_available:
            self._execute_using_filters()
        else:
            self._execute_iterating_all_blocks()

    def _execute_using_filters(self):
        """Uses filters to fetch required logs"""
        start, end = self._get_block_range()
        if start is None:
            return
        all_logs = []

        for (address, topic) in self.decoder.monitored_events.keys():
            log_filter = {
                "topics": [topic],
                "address": address,
                "fromBlock": start,
                "toBlock": end,
            }
            if self.ethereum_logs_filter_getlogs:
                all_logs.extend(self.web3.eth.getLogs(FilterParams(log_filter)))
            else:
                all_logs.extend(self.web3.eth.filter(log_filter).get_all_entries())

        all_logs.sort(key=lambda log: (log["blockNumber"], log["logIndex"]))
        decoded_logs = self.decoder.decode_logs(all_logs)
        self.save_events(decoded_logs)
        self.update_block_number(end)

    def _execute_iterating_all_blocks(self):
        """Executes iterating thru all blocks and txs"""
        pending_blocks = self.get_pending_blocks()
        for block in pending_blocks:
            self.check_for_state_updates(block)
            logs = self.get_block_logs(block)
            decoded_logs = self.decoder.decode_logs(logs)
            self.save_events(decoded_logs)
            self.update_block_number(block)
