from spyne import Application, ServiceBase, Iterable, Unicode, rpc
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from wsgiref.simple_server import make_server
import threading

class MessageService(ServiceBase):
    @rpc(Unicode, _returns=Unicode)
    def send_message(ctx, message):
        # You can process the message here and return a response
        # For simplicity, we echo the message back
        return message


class SOAPService:
    def __init__(self):
        self.app = Application([MessageService], tns='http://example.com/message_service',
                          in_protocol=Soap11(validator='lxml'), out_protocol=Soap11())
        self.wsgi_app = WsgiApplication(self.app)

    def start(self):
        server = make_server('0.0.0.0', 8000, self.wsgi_app)
        server.serve_forever()

    def run(self):
        soap_service_thread = threading.Thread(target=self.start)
        soap_service_thread.daemon = True  # Daemonize the thread (so it will exit when the main thread exits)
        soap_service_thread.start()
