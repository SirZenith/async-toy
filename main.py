from dataclasses import dataclass
import ayty


@dataclass
class Socket():
    readable: bool = False


class UntilSocketReadalbe(ayty.Instruction):
    # Custom instruction
    def __init__(self, socket: Socket):
        self.socket = socket

    def ready(self):
        return self.socket.readable


# Task iterator
def wake_up_call(duration: int):
    """Wait for a while, then wake you up."""
    yield ayty.Sleep(duration)
    print('Rise and shine!')


# Yet another task iterator
def web_request():
    """simulating a time consuming web web web request"""
    skt = Socket()

    def dummy():
        yield ayty.Sleep(6000)
        skt.readable = True
    ayty.add_task(dummy())

    yield UntilSocketReadalbe(skt)

    print('Connect successed.')


# adding iterator into task list
ayty.add_task(wake_up_call(5000))
ayty.add_task(web_request())

# event loop
ayty.loop()
