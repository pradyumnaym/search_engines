from flask import Flask, render_template, request, redirect, url_for

# Run with flask --app website_main run  (--debug)
app = Flask(__name__)

@app.route("/")
def hello_world():
    return render_template('index.html')

@app.route("/search", methods=['POST'])
def search():
    query = request.form.get('query')
    return f"<p>The query was: {query} </p>"