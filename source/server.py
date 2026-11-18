"""本地合成资料来源，仅提供来源行为与观察记录。"""
import argparse
from contextlib import contextmanager
import gzip
import hashlib
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from pathlib import Path
import threading
from urllib.parse import parse_qs, urlsplit

DATA = Path(__file__).resolve().parents[1] / 'data'

class Source:
    def __init__(self):
        self.lock = threading.RLock()
        self.clock = 0
        self.phase = 0
        self.log = []
        self.extra = {}

    def response(self, method, raw_path, headers, body):
        path = urlsplit(raw_path).path
        q = {k:v[-1] for k,v in parse_qs(urlsplit(raw_path).query).items()}
        with self.lock:
            self.log.append({'method':method,'path':raw_path,'time':self.clock,
                             'headers':{k.lower():v for k,v in headers.items()}})
            if path == '/health': return 200, {}, {'来源':'本地合成资料','phase':self.phase,'time':self.clock}
            if path == '/__admin/log': return 200, {}, self.log[:-1]
            if path == '/__admin/advance' and method == 'POST':
                value=json.loads(body or b'{}')
                self.clock += int(value.get('seconds',0))
                self.phase = int(value.get('phase',self.phase))
                return 200, {}, {'phase':self.phase,'time':self.clock}
            if path.startswith('/__admin/'):
                return 404, {}, {'error':'未知演练控制'}
            return self.product(method,path,q,{k.lower():v for k,v in headers.items()},body)

    def inbox(self):
        if 'inbox' not in self.extra:
            rows=json.loads((DATA/'deliveries.json').read_text())
            self.extra['inbox']=[{**x,'done':False,'lease':None,'until':0} for x in rows]
        return self.extra['inbox']
    def product(self,method,path,q,h,body):
        rows=self.inbox()
        if path=='/claim' and method=='POST':
            v=json.loads(body);worker=v['worker']
            for item in rows:
                if not item['done'] and self.clock>=item['until']:
                    counter=self.extra.get('leases',0)+1;self.extra['leases']=counter
                    item.update(lease=f'L-{counter}',until=self.clock+5,worker=worker)
                    return 200,{}, {k:v for k,v in item.items() if k not in ('done',)}
            return 204,{},b''
        if path.startswith('/payload/'):
            p=DATA/'payloads'/path.rsplit('/',1)[-1]
            if self.phase==2:return 503,{}, {'error':'临时读取失败'}
            return 200,{'Content-Type':'application/octet-stream'},p.read_bytes()
        if path in ('/renew','/ack','/reject') and method=='POST':
            v=json.loads(body);item=next(x for x in rows if x['delivery']==v['delivery'])
            if item['done'] or item['lease']!=v['lease'] or self.clock>=item['until']:return 409,{}, {'error':'租约不再有效','done':item['done']}
            if path=='/renew':item['until']=self.clock+5
            else:
                if path=='/reject' and not v.get('reason'):return 400,{}, {'error':'拒收需要依据'}
                item['done']=True;item['disposition']='rejected' if path=='/reject' else 'accepted'
            return 200,{}, {'delivery':item['delivery'],'until':item['until'],'done':item['done']}
        if path=='/status':return 200,{}, {'time':self.clock,'deliveries':rows}
        return 404,{}, {'error':'未知投递操作'}

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self,*args): pass
    def do_GET(self): self.handle_request()
    def do_POST(self): self.handle_request()
    def handle_request(self):
        size=int(self.headers.get('Content-Length','0'))
        try:
            status,headers,body=self.server.source.response(self.command,self.path,self.headers,self.rfile.read(size))
            if not isinstance(body,(str,bytes)):
                body=json.dumps(body,ensure_ascii=False).encode();headers.setdefault('Content-Type','application/json; charset=utf-8')
            elif isinstance(body,str):
                body=body.encode();headers.setdefault('Content-Type','text/html; charset=utf-8')
        except (ValueError,KeyError,TypeError) as exc:
            status,headers,body=400,{'Content-Type':'application/json'},json.dumps({'error':str(exc)},ensure_ascii=False).encode()
        if status==0:
            self.close_connection=True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.send_response(status)
        for k,v in headers.items():self.send_header(k,str(v))
        self.send_header('Content-Length',len(body))
        self.end_headers()
        if body:self.wfile.write(body)

@contextmanager
def running_source(port=0):
    server=ThreadingHTTPServer(('0.0.0.0',port),Handler)
    server.source=Source()
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try: yield 'http://127.0.0.1:'+str(server.server_port),server.source
    finally: server.shutdown();server.server_close();thread.join()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description='启动本地资料来源')
    parser.add_argument('--port',type=int,default=8765)
    args=parser.parse_args()
    server=ThreadingHTTPServer(('0.0.0.0',args.port),Handler);server.source=Source()
    print('资料来源已启动，端口 '+str(args.port),flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.server_close()
