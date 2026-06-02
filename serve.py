import http.server
import socketserver
import os

PORT = 8000
HERE = os.path.dirname(os.path.abspath(__file__))

os.chdir(HERE)
Handler = http.server.SimpleHTTPRequestHandler
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving XaTuring viewer on port {PORT}")
    httpd.serve_forever()
