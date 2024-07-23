from flask import Flask, render_template, request, redirect, url_for
from .. import interface
import pickle

'''
TODO:

- Implement related queries
- Include new dataformat
- Try Iframe stuff
- Maybe Logo


'''

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
#@cross_origin()
def results():
    query = request.args.get('query')
    page = int(request.args.get('page', 1))
    per_page = 10
    total_results = 100

    # results = interface.get_websites(query, count=total_results)
    result = interface.get_websites(query, total_results)
    suggested_queries = result.related_queries[0]
    results = result.results

    # Convert important sentences to one string
    for i in range(len(results)):
        text = ""
        for s in results[i].important_sentences:
            text += s + "\n"
        
        if text == "":
            text = "Sadly there couldn't be found any interesting sentences to display here, if there where any, you could read them now. Instead you just wasted your time reading a lot of nonsens that is just so longto check if hat could cause any problems with formatting."
    
        results[i].important_sentences = text
        
    start = (page - 1) * per_page
    end = start + per_page
    paginated_results = results[start:end]
    
    total_pages = total_results // per_page
    
    return render_template('results.html', query=query, results=paginated_results, page=page, total_pages=total_pages, suggested_queries=suggested_queries)