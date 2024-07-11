from flask import Flask, render_template, request, redirect, url_for
# import interface to use search engine backend
from .. import interface

# Run with flask --app website_main run  (--debug)
app = Flask(__name__)

@app.route("/")
def hello_world():
    return render_template('index.html')

@app.route("/search", methods=['POST'])
def search():
    query = request.form.get('query')
    return redirect(url_for('results', query=query))

@app.route("/results")
def results():
    query = request.args.get('query')
    page = int(request.args.get('page', 1))
    per_page = 10
    total_results = 100

    results = interface.get_websites(query, count=total_results)
    
    start = (page - 1) * per_page
    end = start + per_page
    paginated_results = results[start:end]
    
    total_pages = total_results // per_page
    
    return render_template('results.html', query=query, results=paginated_results, page=page, total_pages=total_pages)