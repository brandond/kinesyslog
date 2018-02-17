import logging
from asyncio import transports, protocols, get_event_loop

from . import constant

logger = logging.getLogger(__name__)


class _ProxyProtocolTransport(transports._FlowControlMixin, transports.Transport):
    """
    Basic wrapped transport inspired by _SSLProtocolTransport.

    Delegates almost everything to the underlying transport, except get_extra_info
    which will substitute Proxy Protocol peerinfo and TLVs if available.
    """
    def __init__(self, loop, proxy_protocol):
        self._loop = loop
        self._proxy_protocol = proxy_protocol
        self._closed = False

    def get_extra_info(self, name, default=None):
        return self._proxy_protocol._get_extra_info(name, default)

    def set_protocol(self, protocol):
        self._proxy_protocol.app_protocol = protocol

    def get_protocol(self):
        return self._proxy_protocol._app_protocol

    def is_closing(self):
        return self._closed

    def close(self):
        self._closed = True

    def __del__(self):
        if not self._closed:
            self.close()

    def is_reading(self):
        return self._proxy_protocol._transport.is_reading()

    def pause_reading(self):
        self._proxy_protocol._transport.pause_reading()

    def resume_reading(self):
        self._proxy_protocol._transport._resume_reading()

    def set_write_buffer_limits(self, high=None, low=None):
        self._proxy_protocol._transport.set_write_buffer_limits(high, low)

    def get_write_buffer_size(self):
        return self._proxy_protocol._transport.get_write_buffer_size()

    @property
    def _protocol_paused(self):
        return self._proxy_protocol._transport._protocol_paused

    def write(self, data):
        self._proxy_protocol._transport.write(data)

    def can_write_eof(self):
        return False

    def abort(self):
        return self._proxy_protocol._transport.abort()


class _BaseProxyProtocol(protocols.Protocol):
    """
    Protocol wrapper inspired by SSLProtocol.

    Once a Proxy Protocol header has been found, the wrapper gets out of the way
    and passes data directly to the app protocol.
    """
    def __init__(self, loop, app_protocol):
        self._loop = loop
        self._app_protocol = app_protocol
        self._app_transport = _ProxyProtocolTransport(self._loop, self)
        self._session_established = False
        self._transport = None
        self._buffer = bytearray()
        self._extra = dict()

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        if self._session_established:
            self._session_established = False
            self._loop.call_soon(self._app_protocol.connection_lost, exc)
        self._transport = None
        self._app_transport = None

    def pause_writing(self):
        self._app_protocol.pause_writing()

    def resume_writing(self):
        self._app_protocol.resume_writing()

    def data_received(self, data):
        if self._session_established:
            self._loop.call_soon(self._app_protocol.data_received, data)
        else:
            self._buffer.extend(data)
            self._parse_proxy_protocol_header()

    def eof_received(self):
        self._transport.close()

    def _get_extra_info(self, name, default=None):
        if name in self._extra:
            return self._extra[name]
        elif self._transport is not None:
            self._transport.get_extra_info(name, default)
        else:
            return default

    def _parse_proxy_protocol_header(self):
        if self._buffer.startswith(constant.PROXY10_MAGIC):
            self._parse_proxy10()
        elif self._buffer.startswith(constant.PROXY20_MAGIC):
            self._parse_proxy20()
        elif len(self._buffer) > len(constant.PROXY20_MAGIC):
            self._close_with_error('PROXY protocol error: invalid header')
        else:
            logger.debug('Waiting for more data...')

    def _parse_proxy10(self):
        header, sep, payload = self._buffer.partition(constant.PROXY10_TERM)
        if sep:
            try:
                header, proto, src_addr, dst_addr, src_port, dst_port = header.split(constant.PROXY10_SEP)
                self._extra['peername'] = (src_addr.decode(), int(src_port))
                self._extra['sockname'] = (dst_addr.decode(), int(dst_port))
                self._start_session(payload)
            except Exception as e:
                self._close_with_error('PROXY protocol error: invalid header: {}'.format(e))

    def _parse_proxy20(self):
        raise NotImplementedError

    def _start_session(self, data):
        self._buffer.clear()
        self._app_protocol.connection_made(self._app_transport)
        logger.debug('Application protocol state: {}'.format(self._app_protocol.__dict__))
        self._session_established = True
        self.data_received(data)

    def _close_with_error(self, message=None):
        if message:
            logger.error(message)
        if self._buffer:
            self._buffer.clear()
        if self._transport:
            self._transport.close()


def wrap(cls):
    """
    Wrap a Server class with Proxy Prococol support
    The base Server and Protocol will be used after Proxy protocol headers have been validated.
    """
    loop = get_event_loop()

    class Protocol(_BaseProxyProtocol):
        def __init__(self, *args, **kwargs):
            super(Protocol, self).__init__(loop=loop, app_protocol=cls.PROTOCOL(*args, **kwargs))

    class Server(cls):
        PROTOCOL = Protocol

        def __init__(self, *args, **kwargs):
            super(Server, self).__init__(*args, **kwargs)

    Protocol.__name__ = 'Proxy'+cls.PROTOCOL.__name__
    Server.__name__ = 'Proxy'+cls.__name__

    return Server
