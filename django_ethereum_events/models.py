import json

from django.core.validators import MinLengthValidator
from django.db import models
try:
    from django.utils.translation import ugettext_lazy as _
except ImportError:
    from django.utils.translation import gettext_lazy as _

CACHE_UPDATE_KEY = '_django_ethereum_events_update_required'
DEFAULT_BLOCKCHAIN_ID = 1


class Daemon(models.Model):
    """Model responsible for storing blockchain related information."""

    blockchain_id = models.PositiveSmallIntegerField(default=DEFAULT_BLOCKCHAIN_ID, primary_key=True)
    blockchain_name = models.CharField(max_length=256, null=True, blank=True)
    block_number = models.IntegerField(default=0, help_text=_('Last block processed'))
    last_error_block_number = models.IntegerField(default=0)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    ethereum_node_timeout = models.PositiveSmallIntegerField(null=True, blank=True)
    ethereum_node_uri = models.CharField(max_length=256, null=True, blank=True)
    ethereum_geth_poa = models.BooleanField(null=True, blank=True)
    ethereum_logs_batch_size = models.PositiveSmallIntegerField(null=True, blank=True)
    ethereum_logs_filter_available = models.BooleanField(null=True, blank=True)
    ethereum_logs_filter_getlogs = models.BooleanField(null=True, blank=True)

    @classmethod
    def get_default_daemon(cls):
        try:
            daemon = Daemon.objects.get(blockchain_id=DEFAULT_BLOCKCHAIN_ID)
        except Daemon.DoesNotExist:
            daemon = Daemon(blockchain_id=DEFAULT_BLOCKCHAIN_ID)
            daemon.save()
        return daemon


class EventManager(models.Manager):
    """Model manager for MonitoredEvent model."""

    @staticmethod
    def register_event(event_name, contract_address, contract_abi, event_receiver, blockchain_id=DEFAULT_BLOCKCHAIN_ID):
        """Helper function that creates a new MonitoredEvent.

        Args:
            event_name (str): the name of the Event that is been emitted
            contract_address (str): the address of the contract emitting the event (hexstring)
            contract_abi (obj): the contract abi either as `str` or `dict`
            event_receiver (str): module in which the event information is passed, must be importable
            blockchain_id (int): blockchain id for multi blockains supporting (default: 1)

        Returns:
            The created MonitoredEvent object

        Raises:
            ValueError if any of the above fields are malformed.
        """
        from .forms import MonitoredEventForm
        form = MonitoredEventForm({
            'name': event_name,
            'contract_address': contract_address,
            'event_receiver': event_receiver,
            'contract_abi': contract_abi,
            'blockchain_id': blockchain_id
        })

        if form.is_valid():
            event = form.save()
            return event

        raise ValueError('The following arguments are invalid \n{0}'.format(form.errors.as_text()))


class MonitoredEvent(models.Model):
    """Holds the events that are currently monitored on the blockchain."""

    daemon = models.ForeignKey(Daemon, default=DEFAULT_BLOCKCHAIN_ID, on_delete=models.CASCADE)
    name = models.CharField(max_length=256)
    contract_address = models.CharField(max_length=42, validators=[MinLengthValidator(42)])
    event_abi = models.TextField()
    topic = models.CharField(max_length=66, validators=[MinLengthValidator(66)])
    event_receiver = models.CharField(max_length=256)
    monitored_from = models.IntegerField(blank=True, null=True,
                                         help_text=_('Block number in which monitoring for this event started'))

    objects = EventManager()

    class Meta:
        verbose_name = _('Monitored Event')
        verbose_name_plural = _('Monitored Events')
        unique_together = ('topic', 'contract_address')

    def __str__(self):
        return '[{0}] {1} at {2}'.format(self.daemon_id, self.name, self.contract_address)

    @property
    def event_abi_parsed(self):
        if hasattr(self, '_event_abi_parsed'):
            return self._event_abi_parsed

        self._event_abi_parsed = json.loads(self.event_abi)
        return self._event_abi_parsed


class FailedEventLog(models.Model):
    """This model holds the event logs that raised an Exception inside the client's event_receiver method.

    When a decode log that is passed inside the client's implementation of the `AbstractEventReceiver`
    raises an exception, the `EventListener` is not halted. Instead, the event log that caused
    the unhandled expeption is stored in this model, along with all the information for the user to
    `replay` the invocation of the custom `event_receiver` implementation.
    """

    event = models.CharField(max_length=256)
    transaction_hash = models.CharField(max_length=66, validators=[MinLengthValidator(66)])
    transaction_index = models.IntegerField()
    block_hash = models.CharField(max_length=66, validators=[MinLengthValidator(66)])
    block_number = models.IntegerField()
    log_index = models.IntegerField()
    address = models.CharField(max_length=42, validators=[MinLengthValidator(42)])
    args = models.TextField(default="{}")  # noqa: P103
    monitored_event = models.ForeignKey(MonitoredEvent, related_name='failed_events', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _('Failed to process Event')
        verbose_name_plural = _('Failed to process Events')

    def __str__(self):
        return self.event
