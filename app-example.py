import httplib2

from apiclient.discovery import build
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials

# BigQuery API Settings
SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_NUMBER = 'xxxx-xxxx-xxxx' # REPLACE WITH YOUR Project ID

# Create a new API service for interacting with BigQuery
credentials = AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
bigquery_service = build('bigquery', 'v2', http=http)


class ListDatasets(webapp.RequestHandler):
  def get(self):
    datasets = bigquery_service.datasets()
    listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
    self.response.out.write('Dataset list:')
    self.response.out.write(listReply)


application = webapp.WSGIApplication(
                                     [('/listdatasets', ListDatasets)],
                                     debug=True)

def main():
  run_wsgi_app(application)

