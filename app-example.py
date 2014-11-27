import httplib2
import json
import pprint
from apiclient.discovery import build
from apiclient.errors import HttpError
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from oauth2client.appengine import AppAssertionCredentials
from oauth2client.client import AccessTokenRefreshError

# BigQuery API Settings
SCOPE = 'https://www.googleapis.com/auth/bigquery'
PROJECT_NUMBER = 'xxxxxxxxxxxxxxxx' # REPLACE WITH YOUR Project ID

# Create a new API service for interacting with BigQuery
credentials = AppAssertionCredentials(scope=SCOPE)
http = credentials.authorize(httplib2.Http())
bigquery_service = build('bigquery', 'v2', http=http)


class ListDatasets(webapp.RequestHandler):
  def get(self):
     try:
       datasets = bigquery_service.datasets()
       listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
       self.response.out.write('Dataset list:')
       self.response.out.write(json.dumps(listReply))

     except HttpError as err:
       print 'Error in listDatasets:', pprint.pprint(err.content)

     except AccessTokenRefreshError:
       print ("Credentials have been revoked or expired, please re-run"
              "the application to re-authorize")

class ExportTable(webapp.RequestHandler):
   def get(self):
     projectId = 'xxxxxxxxxxxxxxxx'
     datasetId = "xxxxxxxxxxxxxxxx"
     tableId = 'xxxxxxxxxxxxxxxx'
     query = "SELECT REGEXP_EXTRACT(user_agent, r'(1.0.\d+)') AS build_version, controller + ':' + action  AS controller_action, time FROM [" + projectId + ":" + datasetId + "." + tableId + "]" 
     url = "https://www.googleapis.com/bigquery/v2/projects/" + projectId + "/jobs"

     jobCollection = bigquery_service.jobs()
     jobData = {
         "projectId": projectId,
         "configuration": {
            "query": {
               "query": query,
               "destinationTable":{
                  "projectId": projectId,
                  "datasetId": datasetId,
                  "tableId": "limited_log"
                  }
            }
         }
      }
     print jobData
     insertJob = jobCollection.insert(projectId=projectId, body=jobData).execute()
     import time
     while True:
       status =jobCollection.get(projectId=projectId, jobId=insertJob['jobReference']['jobId']).execute()
       print status
       if 'DONE' == status['status']['state']:
         print "Done exporting!"
         return
       print 'Waiting for export to complete..'
       time.sleep(10)

app = webapp.WSGIApplication(
    [('/listdatasets', ListDatasets),('/export', ExportTable)],
    debug=True)

def main():
  run_wsgi_app(app)

