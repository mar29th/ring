from struct import calcsize, pack

LEN_MAX_PACKET = 128 * 1024  # KB

FLAG_CONTROL = 1 << 2
FLAG_MORE = 1

MAJOR_VERSION = b'\x11'
MINOR_VERSION = b'\x21'
IDX_MAJOR_VERSION = 4

REQUESTER_GREETING = b'\x00\x00\x00\x00' + MAJOR_VERSION
LEN_REQUESTER_GREETING = len(REQUESTER_GREETING)

REPLIER_GREETING = b'\x00\x00\x00\x00' + MINOR_VERSION
LEN_REPLIER_GREETING = len(REPLIER_GREETING)

FMT_FRAME_HEADER = '>BI'
LEN_FRAME_HEADER = calcsize(FMT_FRAME_HEADER)


def generate_payload_frame(data):
    data = memoryview(data)
    ptr = 0

    while ptr < len(data):
        max_allowable = LEN_MAX_PACKET - LEN_FRAME_HEADER
        body = data[ptr:ptr+max_allowable].tobytes()
        ptr += len(body)

        packet_length = LEN_FRAME_HEADER + len(body)
        flags = FLAG_MORE if ptr < len(data) else 0
        header = pack(FMT_FRAME_HEADER, flags, packet_length)

        yield header + body
