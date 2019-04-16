"""Ring buffers for multiprocessing.

Allows multiple child Python processes started via the multiprocessing module
to read from a shared ring buffer in the parent process. For each child, a
pointer is maintained for the purpose of reading. One pointer is maintained by
for the purpose of writing. Reads may be issued in blocking or non-blocking
mode. Writes are always in non-blocking mode and will raise an exception
if the buffer is full.
"""

import contextlib
import ctypes
import multiprocessing
from typing import Type


class Error(Exception):
    pass


class DataTooLargeError(Error, ValueError):
    pass


class WaitingForReaderError(Error):
    pass


class WaitingForWriterError(Error):
    pass


class WriterFinishedError(Error):
    pass


class AlreadyClosedError(Error):
    pass


class MustCreatedReadersBeforeWritingError(Error):
    pass


class InternalLockingError(Error):
    pass


class InsufficientDataError(Error):
    pass


class Position:
    def __init__(self, slot_count):
        self.counter = 0
        self.slot_count = slot_count

    @property
    def index(self):
        return self.counter % self.slot_count

    @property
    def generation(self):
        return self.counter // self.slot_count


class Pointer:
    def __init__(self, slot_count, *, start=None):
        default = start if start is not None else 0
        self.counter = multiprocessing.RawValue(ctypes.c_longlong, default)
        self.position = Position(slot_count)

    def increment(self):
        self.counter.value += 1

    def increment_by(self, n: int):
        self.counter.value += n

    def get(self):
        # Avoid reallocating Position repeatedly.
        self.position.counter = self.counter.value
        return self.position

    def set(self, counter: int):
        self.counter.value = counter


class RingBuffer:
    """Circular buffer class accessible to multiple threads or child processes.

    All methods are thread safe. Multiple readers and writers are permitted.
    Before kicking off multiprocessing.Process instances, first allocate all
    of the writers you'll need with new_writer() and readers with new_reader().
    Pass the Pointer value returned by the new_reader() method to the
    multiprocessing.Process constructor along with the RingBuffer instance.
    Calling  new_writer() or new_reader() from a child multiprocessing.Process
    will not work.
    """

    def __init__(self, *, c_type: Type[ctypes._SimpleCData], slot_count: int):
        """Initializer.

        Args:
            c_type: Ctypes type of the slot.
            slot_count: How many slots should be in the buffer.
        """
        self.slot_count = slot_count
        self.array = multiprocessing.RawArray(c_type, slot_count)
        self.c_type = c_type
        self.slot_size = ctypes.sizeof(self.c_type)
        self.lock = ReadersWriterLock()
        # Each reading process may modify its own Pointer while the read
        # lock is being held. Each reading process can also load the position
        # of the writer, but not load any other readers. Each reading process
        # can also load the value of the 'active' count.
        self.readers = []
        # The writer can load and store the Pointer of all the reader Pointers
        # or the writer Pointer while the write lock is held. It can also load
        # and store the value of the 'active' acount.
        self.writer = Pointer(self.slot_count)
        self.active = multiprocessing.RawValue(ctypes.c_uint, 0)

    def new_reader(self) -> Pointer:
        """Returns a new unique reader into the buffer.

        This must only be called in the parent process. It must not be
        called in a child multiprocessing.Process. See class docstring. To
        enforce this policy, no readers may be allocated after the first
        write has occurred.
        """
        with self.lock.for_write():
            writer_position = self.writer.get()
            if writer_position.counter > 0:
                raise MustCreatedReadersBeforeWritingError

            reader = Pointer(self.slot_count, start=writer_position.counter)
            self.readers.append(reader)
            return reader

    def new_writer(self) -> None:
        """Must be called once by each writer before any reads occur.

        Should be paired with a single subsequent call to writer_done() to
        indicate that this writer has finished and will not write any more
        data into the ring.
        """
        with self.lock.for_write():
            self.active.value += 1

    def _has_write_conflict(self, position: Position) -> bool:
        index = position.index
        generation = position.generation
        for reader in self.readers:
            # This Position and the other Position both point at the same index
            # in the ring buffer, but they have different generation numbers.
            # This means the writer can't proceed until some readers have
            # sufficiently caught up.
            reader_position = reader.get()
            if (reader_position.index == index
                    and reader_position.generation < generation):
                return True

        return False

    def try_write(self, data: ctypes._SimpleCData) -> None:
        """Tries to write the next slot, but will not block.

        Once a successful write occurs, all pending blocking_read() calls
        will be woken up to consume the newly written slot.

        Args:
            data: Object to write in the next available slot.

        Raises:
            WaitingForReaderError: If all of the slots are full and we need
                to wait for readers to catch up before there will be
                sufficient room to write more data. This is a sign that
                the readers can't keep up with the writer. Consider calling
                force_reader_sync() if you need to force the readers to
                catch up, but beware that means they will miss data.
        """
        with self.lock.for_write():
            if not self.active.value:
                raise AlreadyClosedError

            position = self.writer.get()
            if self._has_write_conflict(position):
                raise WaitingForReaderError

            self.array[position.index] = data
            self.writer.increment()

    def try_write_multiple(self, data: ctypes.Array) -> None:
        with self.lock.for_write():
            if not self.active.value:
                raise AlreadyClosedError

            for item in data:
                position = self.writer.get()
                if self._has_write_conflict(position):
                    raise WaitingForReaderError

                self.array[position.index] = item
                self.writer.increment()

    def _try_read_no_lock(self, reader: Pointer,
                          length: int = 1) -> ctypes._SimpleCData:
        position = reader.get()
        writer_position = self.writer.get()

        if writer_position.counter <= position.counter:
            if not self.active.value:
                raise WriterFinishedError
            else:
                raise WaitingForWriterError

        max_length = writer_position.counter - position.counter

        if max_length < length:
            raise InsufficientDataError

        new_array = (self.c_type * length)()

        remaining = self.slot_count - position.index
        ctypes.memmove(new_array,
                       ctypes.addressof(self.array[position.index]),
                       self.slot_size * min(length, remaining))
        if length > remaining:
            ctypes.memmove(
                ctypes.addressof(new_array[remaining]),
                ctypes.addressof(self.array),
                (length - remaining) * self.slot_size)

        reader.increment_by(length)
        return new_array

    def try_read(self, reader: Pointer, length: int = 1):
        """Tries to read the next slot for a reader, but will not block.

        Args:
            reader: Position previously returned by the call to new_reader().

        Returns:
            ctype array of c_type objects.

        Raises:
            WriterFinishedError: If the RingBuffer was closed before this
                read operation began.
            WaitingForWriterError: If the given reader has already consumed
                all the data in the ring buffer and would need to block in
                order to wait for new data to arrive.
        """
        with self.lock.for_read():
            return self._try_read_no_lock(reader, length=length)

    def try_read_all(self, reader: Pointer):
        with self.lock.for_read():
            length = self.writer.get().counter - reader.get().counter
            return self._try_read_no_lock(reader, length=length)

    def blocking_read(self, reader, length: int = 1, timeout: float = None):
        """Reads the next slot for a reader, blocking if it isn't filled yet.

        Args:
            reader: Position previously returned by the call to new_reader().

        Returns:
            bytearray containing a copy of the data from the slot. This
            value is mutable an can be used to back ctypes objects, NumPy
            arrays, etc.

        Raises:
            WriterFinishedError: If the RingBuffer was closed while waiting
                to read the next operation.
        """
        with self.lock.for_read():
            while True:
                try:
                    if length < 1:
                        length = self.writer.get().counter - reader.get().counter
                    return self._try_read_no_lock(reader, length=length)
                except WaitingForWriterError:
                    if not self.lock.wait_for_write(timeout=timeout):
                        raise

    def force_reader_sync(self):
        """Forces all readers to skip to the position of the writer."""
        with self.lock.for_write():
            writer_position = self.writer.get()

            for reader in self.readers:
                reader.set(writer_position.counter)

            for reader in self.readers:
                reader.get()

    def writer_done(self):
        """Called by the writer when no more data is expected to be written.

        Should be called once for every corresponding call to new_writer().
        Once all writers have called writer_done(), a WriterFinishedError
        exception will be raised by any blocking read calls or subsequent
        calls to read.
        """
        with self.lock.for_write():
            self.active.value -= 1


class ReadersWriterLock:
    """Multiprocessing-compatible Readers/Writer lock.

    The algorithm:
    https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock#Using_a_condition_variable_and_a_mutex

    Background on the Kernel:
    https://www.kernel.org/doc/Documentation/memory-barriers.txt

    sem_wait on Linux uses NPTL, which uses futexes:
    https://github.com/torvalds/linux/blob/master/kernel/futex.c

    Notably, futexes use the smp_mb() memory fence, which is a general write
    barrier, meaning we can assume that all memory reads and writes before
    a barrier will complete before reads and writes after the barrier, even
    if the semaphore / futex isn't actively held.
    """

    def __init__(self):
        self.lock = multiprocessing.Lock()
        self.readers_condition = multiprocessing.Condition(self.lock)
        self.writer_condition = multiprocessing.Condition(self.lock)
        self.readers = multiprocessing.RawValue(ctypes.c_uint, 0)
        self.writer = multiprocessing.RawValue(ctypes.c_bool, False)

    def _acquire_reader_lock(self):
        with self.lock:
            while self.writer.value:
                self.readers_condition.wait()

            self.readers.value += 1

    def _release_reader_lock(self):
        with self.lock:
            self.readers.value -= 1

            if self.readers.value == 0:
                self.writer_condition.notify()

    @contextlib.contextmanager
    def for_read(self):
        """Acquire the lock for reading."""
        self._acquire_reader_lock()
        try:
            yield
        finally:
            self._release_reader_lock()

    def _acquire_writer_lock(self):
        with self.lock:
            while self.writer.value or self.readers.value > 0:
                self.writer_condition.wait()

            self.writer.value = True

    def _release_writer_lock(self):
        with self.lock:
            self.writer.value = False
            self.readers_condition.notify_all()
            self.writer_condition.notify()

    @contextlib.contextmanager
    def for_write(self):
        """Acquire the lock for writing reading."""
        self._acquire_writer_lock()
        try:
            yield
        finally:
            self._release_writer_lock()

    def wait_for_write(self, timeout: float = None):
        """Block until a writer has notified readers.

        Must be called while the read lock is already held. May return
        spuriously before the writer actually did something.
        """
        with self.lock:
            if self.readers.value == 0:
                raise InternalLockingError
            self.readers.value -= 1
            self.writer_condition.notify()
            success = self.readers_condition.wait(timeout=timeout)
            self.readers.value += 1
            return success
