# Copyright (c) 2011-2016 Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ctypes import cdll, util, c_int, c_void_p, c_size_t, \
    c_uint64, byref, get_errno, CDLL, string_at, memmove, c_char_p
import os
import io
import resource


libc = CDLL(util.find_library('c'), use_errno=True)


def open(path, mode='+', buffered=-1):
    if buffered == -1:
        # This size appears on par with kernel buffer sizes
        # and performance is about the same
        buffered = 32768

    os_mode = 0
    if 'o' in mode:
        os_mode = os.O_SYNC
    if 'r' in mode:
        raw = RawDirect(path, mode=os.O_RDONLY | os_mode)
        return io.BufferedReader(raw, buffer_size=buffered)
    if 'w' in mode or 'a' in mode:
        raw = RawDirect(path, mode=os.O_WRONLY | os_mode)
        return io.BufferedWriter(raw, buffer_size=buffered)
    if '+' in mode:
        raw = RawDirect(path, mode=os.O_RDWR | os_mode)
        return io.BufferedRandom(raw, buffer_size=buffered)
    raise ValueError(
        "invalid mode: '%s', must contain atleast one (r,w,+)", mode)


def size(device):
    """ return the size of the device in bytes

        This is fastest most efficient method as os.lseek() calls just
        set the fd->offset, and os.SEEK_END asks the kernel for the
        end of the device """
    fd = os.open(device, os.O_RDONLY)
    try:
        return os.lseek(fd, 0, os.SEEK_END)
    finally:
        os.close(fd)


class RawDirect(io.RawIOBase):

    def __init__(self, path, mode=os.O_RDWR):
        self._fd = os.open(path, os.O_DIRECT | mode)
        self._closed = False
        # There is currently no file system-independent interface
        # for an application to discover the byte alignment restrictions
        # for a given file or file system,  So we default to 512
        self._byte_alignment = 512

        # Tell python about our libc calls
        self._memalign = libc.posix_memalign
        self._memalign.argtypes = [c_void_p, c_size_t, c_size_t]
        self._memalign.errcheck = self.error_check
        self._cread = libc.read
        self._cread.argtypes = [c_int, c_void_p, c_size_t]
        self._cread.errcheck = self.error_check
        self._cwrite = libc.write
        self._cwrite.argtypes = [c_int, c_void_p, c_size_t]
        self._cwrite.errcheck = self.error_check
        self._cfree = libc.free
        self._cfree.argtypes = [c_void_p]
        self.path = path

    def _get_closed(self):
        return self._closed

    closed = property(_get_closed, None, None,
                      "Returns True if the file handle is closed")

    def error_check(self, result, func, args):
        if result < 0:
            errno = get_errno()
            raise OSError(errno, os.strerror(errno))
        return result

    def write(self, buf):
        if isinstance(buf, memoryview):
            buf = buf.tobytes()

        length = len(buf)
        block, remainder = divmod(length, self._byte_alignment)
        # As long as the buf size is a multiple of the byte alignment
        if remainder == 0:
            # Allocate the a mem aligned c buffer
            c_buf = c_void_p()
            self._memalign(byref(c_buf), self._byte_alignment, length)
            # Copy the bytes into the c_buf
            memmove(c_buf, c_char_p(buf), length)
            # Write out the buffer
            write_len = self._cwrite(self._fd, c_buf, length)
            self._cfree(c_buf)
            return write_len

        raise OSError(22, "Refusing to write a buffer of length %d"
                      "; length must be a multiple of %d" %
                      (length, self._byte_alignment))

    def read(self, length=None):
        # If length is -1 or None, call self.readall()
        if length == -1 or length is None:
            return self.readall()

        return self._read(length)[1]

    def _read(self, length):
        block, remainder = divmod(length, self._byte_alignment)
        # As long as the length is a multiple of the byte alignment
        if remainder == 0:
            # Allocate the a mem aligned c buffer
            c_buf = c_void_p()
            self._memalign(byref(c_buf), self._byte_alignment, length)
            length = self._cread(self._fd, c_buf, length)
            # Copy the contents of the c_buf
            string = string_at(c_buf, length)
            # Free the c_buf and return the read value
            self._cfree(c_buf)
            return (length, string)

        raise OSError(22, "Refusing to read buffer of length %d"
                      "; length must be a multiple of %d" %
                      (length, self._byte_alignment))

    def close(self):
        # TODO: Free the memalign buffer (self._buf)
        self._closed = True
        # flush all kernel buffers
        os.fsync(self._fd)
        # close the file
        return os.close(self._fd)

    def readall(self):
        result = []
        while True:
            buf = self.read(32768)
            if len(buf) == 0:
                break
            result.append(buf)
        return ''.join(result)

    def readinto(self, buf):
        length, string = self._read(len(buf))
        buf[0:len(buf)] = string
        return length

    def fileno(self):
        return self._fd

    def flush(self):
        pass

    def isatty(self):
        return False

    def readable(self):
        if not self._closed:
            return True
        return False

    def seekable(self):
        return True

    def tell(self):
        if self._closed:
            return 0
        return os.lseek(self._fd, 0, os.SEEK_CUR)

    def seek(self, offset, whence=os.SEEK_SET):
        # NOTE: Subclasses of BufferedIOBase call seek(-3584)
        # when closing the file, and I've not figured out why
        # Ideally we should NOT allow users to seek to a offset
        # that is NOT a multiple of 512
        return os.lseek(self._fd, offset, whence)

    def truncate(self, size=None):
        return os.ftruncate(self._fd, size)

    def writable(self):
        if self._closed:
            return False
        return True

    def writelines(self, lines):
        raise OSError(0, "writelines() Un-Implemented")
