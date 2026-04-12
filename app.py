from flask import Flask, Response

app = Flask(__name__)

@app.get("/motd")
def motd():
    return Response("Hello from Rec Room MOTD\n", mimetype="text/plain")