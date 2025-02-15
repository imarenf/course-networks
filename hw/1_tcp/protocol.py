import socket
import heapq


PACKET_SIZE = 1500
HEADER_SIZE = 10
DATA_SIZE = PACKET_SIZE - HEADER_SIZE
SEP = HEADER_SIZE // 2


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


class MyTCPHeader:
    def __init__(self, seq_num, ack_num):
        self.seq_num = seq_num
        self.ack_num = ack_num

    def __bytes__(self):
        return self.seq_num.to_bytes(SEP, byteorder='big') + \
                self.ack_num.to_bytes(SEP, byteorder='big')

    @classmethod
    def from_bytes(cls, data):
        seq_num = int.from_bytes(data[:SEP], byteorder='big')
        ack_num = int.from_bytes(data[SEP:], byteorder='big')
        return cls(seq_num, ack_num)


class MyTCPPacket:
    def __init__(self, data, seq_num=None, ack_num=None, header=None):
        if header:
            self.header = header
        else:
            self.header = MyTCPHeader(seq_num, ack_num)
        self.data = data

    def __bytes__(self):
        return bytes(self.header) + self.data

    @classmethod
    def from_bytes(cls, data):
        header = MyTCPHeader.from_bytes(data[:HEADER_SIZE])
        data = data[HEADER_SIZE:]
        return cls(data, header=header)

    def __lt__(self, other):
        return self.header.seq_num < other.header.seq_num


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.send_size = 1
        self.recv_size = 1
        self.queue = []
        self.buffer = b''

    def _send_packet(self, data, seq, ack):
        self.sendto(bytes(MyTCPPacket(
            data,
            seq_num=seq,
            ack_num=ack
        )))
    
    def _ack(self):
        self._send_packet(b'', 0, self.recv_size)
    
    def _validate_transmission(self, data, init_size):
        target_size = init_size + len(data)
        while self.send_size < target_size:
            packet = self._recv_packet()
            header = packet.header
            if header.seq_num != 0:
                self._process_received_packet(packet)
                self._ack()
                continue
            if header.ack_num > self.send_size:
                self.send_size = header.ack_num
                if self.send_size < target_size:
                    offset = self.send_size - init_size
                    self._send_packet(
                        data[offset: offset + DATA_SIZE],
                        self.send_size,
                        self.recv_size
                    )
        self.send_size = target_size

    def _recv_packet(self):
        return MyTCPPacket.from_bytes(self.recvfrom(PACKET_SIZE))
    
    def _process_received_packet(self, packet):
        if packet.header.seq_num == self.recv_size:
            self.buffer += packet.data
            self.recv_size += len(packet.data)
            self._purge()
        else:
            heapq.heappush(self.queue, packet)
    
    def _purge(self):
        while self.queue and self.queue[0].header.seq_num <= self.recv_size:
            packet = heapq.heappop(self.queue)
            self.buffer += packet.data
            self.recv_size += len(packet.data)

    def send(self, data: bytes):
        for i in range(len(data) // DATA_SIZE + 1):
            self._send_packet(
                data[i * DATA_SIZE: (i + 1) * DATA_SIZE],
                self.send_size + i * DATA_SIZE,
                self.recv_size
            )
        self._validate_transmission(data, self.send_size)
        return len(data)

    def recv(self, n: int):
        while len(self.buffer) < n:
            try:
                packet = self._recv_packet()
                self._process_received_packet(packet)
                self._purge()
            except Exception:
                continue
        self._ack()
        data = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return data

    def close(self):
        super().close()

