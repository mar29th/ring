class BufferReader(object):

    def __init__(self, buf):
        self.buffer = buf
        self.read_ptr = 0
        self.buffer_size = 0

    def _unify(self):
        self.buffer[1] = self.buffer[0] + self.buffer[1]
        self.buffer.popleft()

    def add(self, block):
        self.buffer.append(block)
        self.buffer_size += len(block)

    def read_until_delimiter(self, delimiter):
        delimiter_length = len(delimiter)
        while len(self.buffer) >= 1:
            pos = self.buffer[0].find(delimiter, self.read_ptr)
            if pos == -1:
                self.read_ptr = max(0, len(self.buffer[0]) - delimiter_length)
                if len(self.buffer) > 1:
                    self._unify()
                    continue
                else:
                    break
            else:
                popped = self.buffer[0][:pos + delimiter_length]
                self.buffer[0] = self.buffer[0][pos + delimiter_length:]
                if len(self.buffer[0]) == 0:
                    self.buffer.popleft()
                self.read_ptr = 0
                self.buffer_size -= len(popped)
                return popped
        return None

    def read_until_length(self, length):
        if length > self.buffer_size:
            return None
        segments = []
        segments_size = 0
        while True:
            if segments_size + len(self.buffer[0]) <= length:
                segments_size += len(self.buffer[0])
                segments.append(self.buffer.popleft())
            else:
                segments.append(self.buffer[0][:(length - segments_size)])
                self.buffer[0] = self.buffer[0][(length - segments_size):]
                segments_size = length
            if segments_size == length:
                self.buffer_size -= length
                return b''.join(segments)
